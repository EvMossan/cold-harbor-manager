# Contributing Guidelines

This document describes how we work with code, branches, and pull requests
for the Cold Harbor dashboard project.
---

## 1. General principles

* The main development line is the `main` branch.
* Changes land in `main` **only via Pull Requests (PRs)**.
* One PR should correspond to **one logical task** (for example,
  "Add Dockerfile", "Configure Cloud Run deployment", etc.).
* All work should be based on the latest version of `main`.

---

## 2. Branches

Before starting work on a new task, always create a branch from `main`.

1. Update `main` locally:

   ```bash
   git checkout main
   git pull
   ```

2. Create a new branch:

   ```bash
   # For a new feature
   git checkout -b feature/cloud-run

   # For a bug fix
   git checkout -b bugfix/fix-dashboard-timezone

   # For small technical changes (docs, config cleanups, etc.)
   git checkout -b chore/update-readme
   ```

### Branch naming rules

Use the following prefixes:

* `feature/...` – new functionality.
* `bugfix/...` – bug fixes.
* `chore/...` – technical maintenance (docs, formatting, config tweaks).

Try to keep branch names short but descriptive.

---

## 3. Commits

Make small, focused commits. Each commit should represent a meaningful step:

* adding or changing a specific piece of functionality;
* fixing a particular bug;
* updating documentation.

### Commit message style

* Use a short, imperative sentence in English for the title (up to ~72 characters).
* If needed, add an empty line and then a longer description.

Examples of good commit messages:

* `Add Dockerfile and entrypoint script`
* `Configure gunicorn to use Cloud Run PORT`
* `Update README with local run instructions`
* `Fix dashboard date parsing error`

Avoid vague messages like `fix`, `update`, `stuff`, or `wip`.

---

## 4. Pull Requests

When your work in a branch is ready, open a Pull Request (PR) into `main`.

### Scope of a PR

* One PR should ideally solve **one problem** or introduce **one feature**.
* Do not mix unrelated changes (for example, Docker setup and big refactoring of
  business logic) in a single PR.

### PR title and description

* Title: short, clear summary of the change.

  * Examples:

    * `Add Docker build and .dockerignore`
    * `Set up Cloud Run service configuration`
* Description: briefly explain:

  * What was changed.
  * Why it was changed (motivation or issue link, if any).
  * How you tested it (commands, URLs, screenshots if helpful).

Example PR description structure:

```markdown
## What was done
- Added Dockerfile for the Flask dashboard
- Added entrypoint script that runs gunicorn with PORT from env

## How to test
- Build image: `docker build -t dashboard:test .`
- Run container: `docker run --rm -p 5000:5000 dashboard:test`
- Open http://localhost:5000 and verify the dashboard loads
```

### Before requesting review

Before you mark a PR as ready for review, please:

1. Make sure the application starts locally without errors.
2. If relevant for the change:

   * Docker image builds successfully.
   * Container starts and responds on the expected port.
3. Check for obvious style or formatting issues.
4. Update documentation (for example, `README.md`) if behavior or run
   instructions have changed.

---

## 5. Issues

When you create an issue, include enough information so that another person
can reproduce and understand the problem or task.

For **bug reports**, please include:

* Short summary of the problem.
* Steps to reproduce (what you did).
* Expected result.
* Actual result.
* Any relevant logs, error messages, or screenshots.

For **feature requests / tasks**, please include:

* Short description of the desired change.
* Motivation: why this is useful.
* Any constraints or acceptance criteria (what it means for the task to be done).

---

Thank you for contributing and for keeping the workflow clean and consistent!
