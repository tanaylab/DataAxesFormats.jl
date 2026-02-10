#!/usr/bin/env python3

import re
import sys
from pathlib import Path

IMG_PATTERN = r'<img\s+src="(assets/[^"]+\.svg)"[^>]*/>'
SVG_PATTERN = r'<svg width="[^"]*" height="[^"]*"'

def inline_svg_images(html_path, assets_dir):
    html_content = html_path.read_text(encoding='utf-8')

    def replace_with_svg(img_match):
        svg_path_str = img_match.group(1)
        svg_path = html_path.parent / svg_path_str
        svg_content = svg_path.read_text(encoding='utf-8')
        return re.sub(SVG_PATTERN, '<svg width="100%" height="100%"', svg_content)

    new_content = re.sub(IMG_PATTERN, replace_with_svg, html_content)
    if new_content != html_content:
        html_path.write_text(new_content, encoding='utf-8')


docs_path = Path(sys.argv[1])
html_files = list(docs_path.rglob("*.html"))
for html_file in html_files:
    inline_svg_images(html_file, docs_path)
