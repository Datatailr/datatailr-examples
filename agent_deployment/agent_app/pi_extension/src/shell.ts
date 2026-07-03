export interface CommandResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

export interface CommandRunner {
  exec(command: string, args: string[], options?: { cwd?: string }): Promise<CommandResult>;
}
