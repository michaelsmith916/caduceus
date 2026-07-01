.PHONY: build test lint dev-up package

build:
	./scripts/build.sh

test:
	./scripts/test.sh

lint:
	./scripts/lint.sh

dev-up:
	./scripts/dev-up.sh

package:
	./scripts/package.sh
