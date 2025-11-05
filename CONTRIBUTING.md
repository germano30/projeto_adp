# Contributing

Thanks for your interest in contributing to this project! This file gives a short, practical guide to get you started.

Please read the main `README.md` first for project purpose and quick-start instructions.

## Reporting issues

- Use GitHub Issues to report bugs or request features. Provide a concise title and a minimal reproduction (commands, sample input, and relevant logs).
- When possible, include the Python version and OS.

## Before you code

1. Fork the repository and create a feature branch from `main`:

   - feature branches: `feature/<short-description>`
   - bugfix branches: `fix/<short-description>`

2. Keep your changes focused. One change = one pull request.

3. Update or add tests for any new behavior when feasible.

## Code style

- Follow PEP 8 for Python code. We recommend using `black` and `isort` to format imports.
- Keep functions and modules small and focused. Add docstrings to public functions.

## Tests

- The project uses `pytest`. To run tests locally:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

- If you add functionality, add a small test that verifies the behavior.

## Pull requests

1. Push your branch to your fork and open a Pull Request against `main`.
2. In the PR description, include:
   - A short summary of the change
   - Why the change is needed
   - Any migration or config steps (if applicable)
3. The PR checklist:
   - [ ] Tests added or updated (if applicable)
   - [ ] Code formatted (black/isort)
   - [ ] No secrets or credentials committed

## Security

- Do not open issues containing security vulnerabilities. If you discover a security issue, please contact the repository owner directly rather than posting public details.

## Maintainers

- Repository owner: `germano30` (see `README.md` for contact/maintainer guidance).

## Other notes

- This CONTRIBUTING.md is intentionally short. For more detailed governance (CLA, code of conduct, or commit signing), add files like `CODE_OF_CONDUCT.md` or `LICENSE` at the repository root.

Thanks — contributions are welcome!
