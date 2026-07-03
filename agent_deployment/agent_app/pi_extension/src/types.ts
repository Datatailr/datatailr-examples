export type ComponentKind = "app" | "service" | "workflow" | "excel";

export interface ComponentSpec {
  kind: ComponentKind;
  slug: string;
  displayName: string;
  packageName: string;
  directory: string;
  deployScript: string;
  jobName: string;
}

export interface SystemSpec {
  slug: string;
  displayName: string;
  summary: string;
  multiComponent: boolean;
  components: ComponentSpec[];
}

export interface SubagentRecord {
  id: string;
  title: string;
  branch?: string;
  prUrl?: string | null;
  status?: string;
}

export interface JobRecord {
  componentSlug: string;
  kind: ComponentKind;
  jobName: string;
  deployScript: string;
}

export interface SystemManifest {
  version: 1;
  spec: SystemSpec;
  generatedAt: string;
  subagents: SubagentRecord[];
  jobs: JobRecord[];
}
