"""Points links in files included from the repository root at their site pages or GitHub."""

import re

from markdown import Extension
from markdown.preprocessors import Preprocessor

REPO = "https://github.com/sayef/kv0/blob/main/"
PAGES = {"CHANGELOG.md": "changelog.md", "CONTRIBUTING.md": "contributing.md", "DESIGN.md": "design.md"}
LINK = re.compile(
    r"\]\((?:\.\./)*(CHANGELOG\.md|CONTRIBUTING\.md|DESIGN\.md|SECURITY\.md|CODE_OF_CONDUCT\.md|LICENSE|TODO\.md"
    r"|bench/[A-Za-z0-9_./-]+|assets/[A-Za-z0-9_./-]+|brand/[A-Za-z0-9_./-]+|crates/[A-Za-z0-9_./-]+)(#[^)]*)?\)"
)


# Root files that link into docs/ are included by pages at the top of docs/.
DOCS_LINK = re.compile(r"\]\(docs/([A-Za-z0-9_./-]+\.md)(#[^)]*)?\)")


# Directory depth of the page being rendered, so site links work from pages in subdirectories.
_depth = 0


class RepoLinks(Preprocessor):
    def run(self, lines):
        def target(match):
            name, anchor = match.group(1), match.group(2) or ""
            page = PAGES.get(name)
            return f"]({'../' * _depth + page if page else REPO + name}{anchor})"

        def docs(match):
            return f"]({match.group(1)}{match.group(2) or ''})"

        return [DOCS_LINK.sub(docs, LINK.sub(target, line)) for line in lines]


class RepoLinksExtension(Extension):
    def extendMarkdown(self, md):
        md.preprocessors.register(RepoLinks(md), "kv0_repo_links", 20)


def on_page_markdown(markdown, page, **kwargs):
    global _depth
    _depth = page.file.src_uri.count("/")
    return markdown


def on_config(config):
    config.markdown_extensions.append(RepoLinksExtension())
    return config
