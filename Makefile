.PHONY: help diagrams

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

diagrams:  ## Regenerate architecture.svg and validate Mermaid sources
	python3 docs/diagrams/src/architecture.py
	@for f in docs/diagrams/mermaid/*.mmd; do \
		npx -y -p @mermaid-js/mermaid-cli@11 mmdc -q -i $$f -o /tmp/fcp-$$(basename $$f .mmd).svg || exit 1; \
		echo "ok  $$f"; \
	done
