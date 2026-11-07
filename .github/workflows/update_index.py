import json
import re
import os
import sys

plugin_id = os.environ["PLUGIN_ID"]
version   = os.environ["VERSION"]
date      = os.environ["DATE"]
zip_path  = os.environ["ZIP"]
sha256    = os.environ["SHA"]

descriptions = {
    "merge_multipart":     "Links multipart scenes into Groups ordered by part number",
    "extrafanart_gallery": "Links extrafanart folders to parent scene galleries",
    "performer_name_sync": "Sets romanized performer names from JavStash and enriches from StashDB",
    "performer_cleanup": "One-time performer cleanup that merges, links, fills scenes, flags duplicates and sets Identify defaults",
}
names = {
    "merge_multipart":     "merge_multipart",
    "extrafanart_gallery": "extrafanart_gallery",
    "performer_name_sync": "performer_name_sync",
    "performer_cleanup": "performer_cleanup",
}

if plugin_id not in descriptions:
    print(f"ERROR: Unknown plugin_id '{plugin_id}'", file=sys.stderr)
    sys.exit(1)

new_block = (
    f"- id: {plugin_id}\n"
    f"  name: {names[plugin_id]}\n"
    f"  metadata:\n"
    # json.dumps quotes the text so ":" or "#" can never break the YAML
    f"    description: {json.dumps(descriptions[plugin_id])}\n"
    f'  version: "{version}"\n'
    f'  date: "{date}"\n'
    f"  path: {zip_path}\n"
    f"  sha256: {sha256}"
)

try:
    with open("index.yml", "r") as f:
        content = f.read()
except FileNotFoundError:
    content = ""

pattern = rf"- id: {re.escape(plugin_id)}\b.*?(?=\n- id:|\Z)"
if re.search(pattern, content, flags=re.DOTALL):
    updated = re.sub(pattern, new_block, content, flags=re.DOTALL)
else:
    updated = content.rstrip("\n") + ("\n\n" if content.strip() else "") + new_block + "\n"

with open("index.yml", "w") as f:
    f.write(updated)

print(f"Updated index.yml for {plugin_id} -> {version}")
