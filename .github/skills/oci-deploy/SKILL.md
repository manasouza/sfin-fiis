# Skill: GitHub Actions -> Deploy to OCI VM

## Purpose
Provide a reproducible, repo-scoped workflow description and checklist for deploying a project from GitHub Actions to an Oracle Cloud Infrastructure (OCI) VM instance over SSH. The skill documents a step-by-step deployment flow for Python projects (example), but is written to be adaptable to other languages and runtimes.

## Scope
- Workspace-scoped: intended to be added to a repo to guide automated deployments.
- Primary transport: SSH + SCP (GitHub Actions runner -> OCI VM).
- Target runtime: Python (virtualenv), but includes guidance for Node, Java, or systemd-managed services.

## When to use
- You want a simple CI trigger that deploys code to an OCI VM and runs it there.
- You manage single VM instances or small fleets and prefer SSH-based updates over container registries.

## Step-by-step workflow
1. Trigger
   - Trigger on `push` to a deployment branch (e.g., `main` or `deployment-byassist`) and support manual `workflow_dispatch` runs.

2. Checkout
   - Use `actions/checkout@v4` to fetch the repository contents.

3. Authenticate to VM
   - Use an SSH key for authentication. Add secrets: `OCI_VM_HOST`, `OCI_VM_USER`, `OCI_VM_SSH_KEY`, and `OCI_VM_KNOWN_HOSTS`.
   - Optionally support instance principal, OCI bastion, or a GitHub-hosted runner inside the same VCN.

4. Copy files
   - Use `appleboy/scp-action` (or `rsync` over SSH) to transfer the repository to a target directory on the VM, e.g. `/home/<user>/sfin-fiis`.

5. Prepare runtime on VM (idempotent)
   - Create a centralized virtualenv under the home directory (recommended): `~/pyenvs/<project-name>`.
   - Only create the venv if it does not exist (idempotent). Use `python -m venv`.
   - Activate the venv and upgrade `pip`.
   - Install dependencies from `requirements.txt` (adjust for other runtimes: `npm ci`, `mvn -q -DskipTests package`, etc.).

6. Run the application
   - Activate the same venv and execute the command, e.g. `python main_local.py -m collected -f '{}'`.
   - Consider running under a process manager (`systemd`, `supervisord`, `pm2`) for long-running services.

7. Tear down / cleanup (optional)
   - Remove temporary files, rotate logs, or rotate secrets if needed.

## Decision points & branching logic
- Authentication method
  - SSH key (simple) vs. OCI Instance Principal (more secure) vs. Bastion host.
- Deployment target path
  - Repo copy under user home vs. a shared app directory (e.g., `/opt/<app>`).
- Virtual environment management
  - Project-local `venv/` vs. centralized `~/pyenvs/<project>` (centralized simplifies shared maintenance).
- Service management
  - One-shot run vs. daemonized run under `systemd` (choose daemonization for production).

## Quality criteria and completion checks
- Secrets present: workflow should fail early (or detect and exit) if required secrets are missing.
- Idempotence: running the workflow multiple times should not create duplicate artifacts or fail due to existing resources (venv, files).
- Exit status: the deployment job should return exit code 0 when the app starts successfully.
- Verification: optionally run a smoke-check (curl to localhost:PORT, or check exit code of `python main_local.py`).
- Logs: deploy logs are available in the GitHub Actions run and on the target VM (e.g., `/var/log/<app>.log`).

## Security considerations
- Store private keys in GitHub Secrets. Never echo them to logs.
- Use `known_hosts` verification to avoid MITM attacks: populate `OCI_VM_KNOWN_HOSTS` with `ssh-keyscan` output for the VM.
- Limit the deployment user's privileges on the VM (avoid running as root).
- For production, consider using OCI Instance Principal or a secured bastion rather than exposing SSH keys broadly.

## Extending to other languages / runtimes
- Node.js: create an `nvm`/`node` environment or use system `node`, then `npm ci` and `npm run start` or `pm2 start`.
- Java: use JDK on VM, build artifact (`mvn package`) on runner or VM, then deploy JAR and run via `java -jar` or `systemd`.
- Containerized: build Docker image in CI and push to a registry, then pull and run on the VM or a container host.

## Example GitHub Actions snippet (Python)
```yaml
# simplified example
- name: Copy files to VM
  uses: appleboy/scp-action@v0.1.4
  with:
    host: ${{ secrets.OCI_VM_HOST }}
    username: ${{ secrets.OCI_VM_USER }}
    key: ${{ secrets.OCI_VM_SSH_KEY }}
    source: "."
    target: "/home/${{ secrets.OCI_VM_USER }}/sfin-fiis"

- name: Install dependencies and run
  uses: appleboy/ssh-action@v1.0.0
  with:
    host: ${{ secrets.OCI_VM_HOST }}
    username: ${{ secrets.OCI_VM_USER }}
    key: ${{ secrets.OCI_VM_SSH_KEY }}
    script: |
      cd /home/${{ secrets.OCI_VM_USER }}/sfin-fiis
      PYENV_DIR="/home/${{ secrets.OCI_VM_USER }}/pyenvs/sfin-fiis"
      if [ ! -d "$PYENV_DIR" ]; then
        python -m venv "$PYENV_DIR"
      fi
      source "$PYENV_DIR/bin/activate"
      python -m pip install --upgrade pip
      pip install -r requirements.txt
      python main_local.py -m collected -f '{}'
```

## Ambiguities & clarifying questions
- Preferred deployment branch name and trigger policy (only `main` or a deployment branch)?
- Do you prefer centralized venv path (recommended) or project-local `venv/`?
- Should the app be run as a one-off job or managed by `systemd`/`supervisord` for continuous operation?
- Do you want the workflow to support passing runtime arguments via workflow inputs or environment variables?

## Example prompts to use this skill
- "Create a GitHub Actions workflow to deploy this repo to an OCI VM using SSH and a centralized venv at `~/pyenvs/<project>`"
- "Update the deployment workflow to run my app under `systemd` and restart the service after deployment"
- "Add smoke test step to the OCI deploy workflow that curls localhost:8080 and fails the job if not healthy"

## Suggested next customizations
- Add a `workflow_dispatch` input for `mode` and `fiis` to pass to `main_local.py` dynamically.
- Add a smoke-test step to verify application health post-deploy.
- Add a `systemd` service unit template and an optional step to install/enable the service.

## How to save and use
1. Add this `SKILL.md` to your repository in `.github/skills/oci-deploy/SKILL.md`.
2. Use it as a checklist when authoring `/.github/workflows/deploy-oci.yml`.
3. Iterate: run the workflow, note failures, and update the skill with any environment- or project-specific tweaks.

---

Author: automation (assistant) — template generated from the current conversation about OCI/GitHub Actions deployment
