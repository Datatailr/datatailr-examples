import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { createManifest, saveManifest } from "./state.ts";
import type { ComponentKind, ComponentSpec, SystemSpec } from "./types.ts";

function componentBaseDir(systemSlug: string, kind: ComponentKind): string {
  return join("systems", systemSlug, kind);
}

export function defaultComponentSpec(systemSlug: string, kind: ComponentKind): ComponentSpec {
  const packageName = `${systemSlug}_${kind}`;
  return {
    kind,
    slug: kind,
    displayName: `${systemSlug} ${kind}`,
    packageName,
    directory: componentBaseDir(systemSlug, kind),
    deployScript: join(componentBaseDir(systemSlug, kind), "deploy.py"),
    jobName: `${systemSlug}-${kind}`,
  };
}

export function buildComponentFiles(component: ComponentSpec): Array<{ path: string; content: string }> {
  const pkgDir = join(component.directory, component.packageName);
  const initFile = { path: join(pkgDir, "__init__.py"), content: "" };

  if (component.kind === "app") {
    return [
      initFile,
      {
        path: join(pkgDir, "app.py"),
        content: `from flask import Flask\n\napp = Flask(__name__)\n\n@app.route("/")\ndef index():\n    return {"status": "ok"}\n`,
      },
      {
        path: join(component.directory, "deploy.py"),
        content: `from ${component.packageName}.app import app\nfrom datatailr import App, Resources\n\njob = App(name="${component.jobName}", entrypoint=app, framework="flask", resources=Resources(memory="512m", cpu=1), python_requirements=["flask"])\n\nif __name__ == "__main__":\n    job.run()\n`,
      },
    ];
  }

  if (component.kind === "service") {
    return [
      initFile,
      {
        path: join(pkgDir, "app.py"),
        content: `from flask import Flask\n\napp = Flask(__name__)\n\n@app.route("/health")\ndef health():\n    return "OK"\n\ndef main(port):\n    app.run("0.0.0.0", port=int(port), debug=False)\n`,
      },
      {
        path: join(component.directory, "deploy.py"),
        content: `from ${component.packageName}.app import main\nfrom datatailr import Resources, Service\n\nservice = Service(name="${component.jobName}", entrypoint=main, resources=Resources(memory="1g", cpu=1), python_requirements=["flask"])\n\nif __name__ == "__main__":\n    service.run()\n`,
      },
    ];
  }

  if (component.kind === "workflow") {
    return [
      initFile,
      {
        path: join(pkgDir, "tasks.py"),
        content: `from datatailr import task\n\n@task()\ndef fetch_input() -> dict:\n    return {"value": 1}\n\n@task()\ndef transform(data: dict) -> dict:\n    data["value"] += 1\n    return data\n`,
      },
      {
        path: join(component.directory, "deploy.py"),
        content: `from datatailr import workflow\nfrom ${component.packageName}.tasks import fetch_input, transform\n\n@workflow(name="${component.jobName}", python_requirements=[])\ndef ${component.packageName}_workflow():\n    transform(fetch_input())\n\nif __name__ == "__main__":\n    ${component.packageName}_workflow()\n`,
      },
    ];
  }

  return [
    initFile,
    {
      path: join(pkgDir, "addin.py"),
      content: `from datatailr.excel import Addin\n\naddin = Addin("${component.displayName}", "${component.displayName} add-in")\n\n@addin.expose(description=\"Adds two numbers\", help=\"Provide a and b\")\ndef add(a: float, b: float) -> float:\n    return a + b\n\ndef main(port=8080, ws_port=8000):\n    addin.run(port, ws_port)\n`,
    },
    {
      path: join(component.directory, "deploy.py"),
      content: `from ${component.packageName}.addin import main\nfrom datatailr import ExcelAddin, Resources\n\naddin = ExcelAddin(name="${component.jobName}", entrypoint=main, resources=Resources(memory=\"4g\", cpu=1), python_requirements=[])\n\nif __name__ == "__main__":\n    addin.run()\n`,
    },
  ];
}

export async function scaffoldSystem(cwd: string, spec: SystemSpec): Promise<{ manifestPath: string; files: string[] }> {
  const files = spec.components.flatMap(buildComponentFiles);
  for (const file of files) {
    const absolute = join(cwd, file.path);
    await mkdir(dirname(absolute), { recursive: true });
    await writeFile(absolute, file.content);
  }
  const manifestPath = await saveManifest(cwd, createManifest(spec));
  return { manifestPath, files: files.map((file) => file.path) };
}
