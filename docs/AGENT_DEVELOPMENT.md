# Agent Development Workflow

This repository is in a transition from the original urban heatwave forecaster
to a modular climate-extremes platform. Agent work should keep that transition
controlled, reviewable, and easy to reverse.

## Core Rules

- Make one scoped change per loop.
- Inspect the relevant files before editing.
- Keep commits grouped by intent.
- Run the full test suite after every change:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -p no:cacheprovider
```

- Do not modify generated data, cache files, output images, notebooks, or demo
  artifacts unless the user explicitly asks for that.
- Do not stage files under `data/`, `outputs/`, `demo/`, `.cache/`, or
  `.pytest_cache/`.
- Do not make broad refactors without approval.
- Prefer small compatibility-preserving changes while the legacy
  `urban_heatwave_forecaster` package still exists.

## Standard Loop

1. Inspect: read the current tree, status, nearby tests, and relevant modules.
2. State scope: name the exact files and behavior the loop intends to touch.
3. Edit narrowly: change only what is needed for that scope.
4. Test: run the full pytest command.
5. Check staged content before commit:

```bash
git diff --cached --check
git diff --cached --name-only -- data outputs demo .cache .pytest_cache
```

6. Commit with a message that describes the actual change.
7. Report commit hash, files changed, tests run, and remaining dirty state.

## Stop Conditions

Stop and report instead of continuing when any of these happen:

- Tests fail and the fix would require broad or unrelated edits.
- Generated data, cache files, output images, notebooks, or demo artifacts are
  staged.
- The requested change conflicts with existing unstaged user work.
- The change requires new platform scope, new hazards, or new product behavior.
- The implementation depends on network calls that cannot be mocked or avoided.
- The intended commit includes mixed concerns that should be split.

## Recommended Prompts

### Audit Loop

```text
Audit the repository without modifying files. Identify current structure,
legacy heatwave components, climate_extremes platform components, incomplete
or confusing code, current test status, and the smallest safe next step.
```

### Fix Loop

```text
Fix only the failing behavior in <area>. Inspect nearby code first, keep the
change minimal, run the full pytest command, and commit only the relevant
source and test files. Do not touch generated artifacts.
```

### Refactor Loop

```text
Refactor <specific module/function> without changing behavior. Preserve public
interfaces, add or update focused tests if needed, run the full pytest command,
and commit only the refactor files. Stop if the refactor spreads beyond the
named scope.
```

### Documentation Loop

```text
Update documentation for <specific workflow or module>. Do not change runtime
code. Verify links and commands against the current repo. Run tests if any
examples or command contracts changed.
```

## Repository-Specific Notes

- `src/urban_heatwave_forecaster/` is the legacy compatibility surface.
- `src/climate_extremes/` is the modular platform surface.
- `src/climate_extremes/modules/heat/` is the stable heat module.
- `src/climate_extremes/modules/precipitation/` is still experimental and
  should remain clearly labeled as candidate science until validated.
- `app.py` is a Streamlit adapter and should stay thin over backend services
  where practical.
