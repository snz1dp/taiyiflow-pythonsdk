.DEFAULT_GOAL := help

.PHONY: help install install-dev test test-offline test-integration lint format format-check typecheck check build clean docs-init docs-dev docs-build docs-build-gh docs-preview

ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
DOCS_DIR := $(ROOT_DIR)/docs
PYTHON ?= python3
NPM ?= npm
DOCS_HOST ?= 127.0.0.1
DOCS_PORT ?= 5173
DOCS_BASE_PATH ?= /taiyi/pythonsdk/docs/

help:
	@printf '%s\n' \
		'make install          安装核心包（可编辑模式）' \
		'make install-dev      安装开发与测试依赖' \
		'make test             运行全部测试' \
		'make test-offline     排除真实环境集成测试' \
		'make test-integration 运行真实环境集成测试（需显式配置）' \
		'make lint             运行 Ruff 静态检查' \
		'make format           格式化代码与文档代码块' \
		'make format-check     检查格式' \
		'make typecheck        运行 Mypy strict 检查' \
		'make check            运行测试、Ruff、格式与 Mypy 门禁' \
		'make build            构建 wheel 与 sdist' \
		'make docs-init        安装文档依赖' \
		'make docs-dev         启动文档开发服务器' \
		'make docs-build       构建文档并检查死链' \
		'make docs-build-gh    构建 GitHub Pages 文档' \
		'make docs-preview     预览已构建文档' \
		'make clean            清理 Python 构建与测试缓存'

install:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m pip install -e .

install-dev:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m pip install -e '.[dev]'

test:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m pytest

test-offline:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m pytest -m 'not integration'

test-integration:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m pytest -m integration

lint:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m ruff check .

format:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m ruff format .

format-check:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m ruff format --check .

typecheck:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m mypy src

check: test lint format-check typecheck

build:
	cd "$(ROOT_DIR)" && "$(PYTHON)" -m build

docs-init:
	cd "$(DOCS_DIR)" && "$(NPM)" install

docs-dev:
	cd "$(DOCS_DIR)" && "$(NPM)" run docs:dev -- --host "$(DOCS_HOST)" --port "$(DOCS_PORT)"

docs-build:
	cd "$(DOCS_DIR)" && VITEPRESS_BASE_PATH="$(DOCS_BASE_PATH)" "$(NPM)" run docs:build

docs-build-gh:
	cd "$(DOCS_DIR)" && "$(NPM)" run docs:build:gh

docs-preview:
	cd "$(DOCS_DIR)" && "$(NPM)" run docs:preview -- --host "$(DOCS_HOST)" --port "$(DOCS_PORT)"

clean:
	rm -rf "$(ROOT_DIR)/build" "$(ROOT_DIR)/dist" \
		"$(ROOT_DIR)/.pytest_cache" "$(ROOT_DIR)/.mypy_cache" \
		"$(ROOT_DIR)/.ruff_cache" "$(ROOT_DIR)/htmlcov"
	rm -f "$(ROOT_DIR)/.coverage"
