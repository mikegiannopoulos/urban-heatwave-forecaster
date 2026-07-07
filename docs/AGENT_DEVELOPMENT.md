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

## Scientific Integrity

Separate software refactoring from scientific changes. Refactoring may improve
structure, naming, tests, typing, or interfaces, but it must preserve scientific
behavior unless the user explicitly asks for a scientific change.

Agents must not modify these scientific components unless explicitly instructed:

- Hazard definitions
- Detection thresholds
- Climatological baselines
- Severity scales
- Statistical methods
- Risk calculations

Any scientific change must include:

- Rationale for the change
- Affected modules and public outputs
- Updated documentation
- Updated tests that cover the changed behavior

## Never Invent Science

Agents must never invent thresholds, climatologies, validation metrics, or
literature support. If required scientific information is missing, stop and
report what is missing instead of guessing.

## Layered Architecture

The intended dependency direction is:

```text
Core
  ↓
IO
  ↓
Hazard Modules
  ↓
Shared Risk Model
  ↓
CLI / Streamlit
```

Lower layers must never depend on higher layers. For example, `core` must not
import hazard modules, hazard modules must not import Streamlit, and backend
workflow code should not depend on UI formatting helpers.

## Future Hazard Modules

Future hazards should follow the existing module pattern used by `heat` and
`precipitation`. Examples include `drought`, `wildfire`, and `coastal`.

Each hazard module should keep its science and workflow local:

- `profiles.py` for explicit candidate definitions when needed
- `baselines.py` for hazard-specific climatology or reference builders
- `detection.py` for event detection
- `risk.py` for hazard-specific severity and shared assessment mapping
- `workflow.py` for file-producing backend orchestration
- Tests for detection, risk, summaries, and workflow behavior

New modules should emit the shared `HazardAssessment` contract rather than
forcing all hazards into one premature total-risk formula.

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
- The change would alter scientific behavior without explicit instruction.
- Scientific rationale, source information, or validation criteria are missing.
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
source and test files. Do not touch generated artifacts or scientific
parameters unless explicitly instructed.
```

### Refactor Loop

```text
Refactor <specific module/function> without changing behavior. Preserve public
interfaces, add or update focused tests if needed, run the full pytest command,
and commit only the refactor files. Stop if the refactor spreads beyond the
named scope or changes scientific outputs.
```

### Documentation Loop

```text
Update documentation for <specific workflow or module>. Do not change runtime
code. Verify links and commands against the current repo. Run tests if any
examples or command contracts changed.
```

### Scientific Change Loop

```text
Make the explicitly requested scientific change to <hazard/module>. Document
the rationale, affected modules, changed thresholds/methods/calculations,
updated tests, and any limits or validation gaps. Stop if source information is
missing.
```

## Repository-Specific Notes

- `src/urban_heatwave_forecaster/` is the legacy compatibility surface.
- `src/climate_extremes/` is the modular platform surface.
- `src/climate_extremes/modules/heat/` is the stable heat module.
- `src/climate_extremes/modules/precipitation/` is still experimental and
  should remain clearly labeled as candidate science until validated.
- `src/climate_extremes/core/` owns shared contracts, locations, paths, events,
  severity scales, and summaries.
- `src/climate_extremes/io/` owns external data access and should stay separate
  from hazard science.
- `app.py` is a Streamlit adapter and should stay thin over backend services
  where practical.
