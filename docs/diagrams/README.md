# Diagrams

| Diagram | Source | Used in |
|---|---|---|
| System architecture | [`src/architecture.py`](src/architecture.py) → [`architecture.svg`](architecture.svg) | README, ARD §1 |
| Local deployment | [`mermaid/deployment.mmd`](mermaid/deployment.mmd) | ARD §5 |
| Data model (ER) | [`mermaid/data-model.mmd`](mermaid/data-model.mmd) | SDD §2 |
| Change → re-index sequence | [`mermaid/reindex-sequence.mmd`](mermaid/reindex-sequence.mmd) | SDD §4 |
| Agent query sequence | [`mermaid/agent-sequence.mmd`](mermaid/agent-sequence.mmd) | SDD §6 |
| Freshness label decision | [`mermaid/label-decision.mmd`](mermaid/label-decision.mmd) | SDD §6 |
| Evaluation protocol | [`mermaid/eval-protocol.mmd`](mermaid/eval-protocol.mmd) | SDD §7 |

## Regenerating

```bash
make diagrams
```

This regenerates `architecture.svg` and validates every `.mmd` file with mermaid-cli. The Mermaid blocks inside the docs are copies of the `.mmd` files, so edit the `.mmd` file first and then copy it into the doc.
