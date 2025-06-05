import os
import shutil
from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

HELLO_WORLD_FILES = [
    {"template": "logger.jinja",      "dest": "src/utils/logger.py"},
    {"template": "hello_task.jinja",  "dest": "src/tasks/hello_task.py"},
    {"template": "hello_flow.jinja",  "dest": "src/flows/hello_flow.py"},
    {"template": "config.jinja",      "dest": "src/config/config.py"},
    {"template": "hello.sql.jinja",   "dest": "src/sql/bronze/hello.sql"},
    {"template": "test_hello.jinja",  "dest": "src/tests/test_hello.py"},
    {"template": "README.md.jinja",   "dest": "README.md"},
    # Add more as needed
]

def create_base_structure(target_dir):
    dirs = [
        "src/utils", "src/tasks", "src/flows",
        "src/config", "src/sql/bronze",
        "src/tests"
    ]
    for d in dirs:
        os.makedirs(os.path.join(target_dir, d), exist_ok=True)

def render_templates(target_dir, context):
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    for file in HELLO_WORLD_FILES:
        template = env.get_template(file["template"])
        dest_path = os.path.join(target_dir, file["dest"])
        with open(dest_path, "w") as f:
            f.write(template.render(**context))

def copy_requirements(target_dir):
    # Optionally, provide a starter requirements.txt
    req_path = os.path.join(TEMPLATE_DIR, "requirements.txt")
    if os.path.exists(req_path):
        shutil.copy(req_path, os.path.join(target_dir, "requirements.txt"))

def init_project(project_name):
    target_dir = os.path.abspath(project_name)
    if os.path.exists(target_dir):
        raise FileExistsError(f"Directory {target_dir} already exists!")
    os.makedirs(target_dir)
    create_base_structure(target_dir)
    context = {"project_name": project_name}
    render_templates(target_dir, context)
    copy_requirements(target_dir)
    print(f"Project {project_name} initialized at {target_dir}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m transmute.init <project_name>")
        sys.exit(1)
    init_project(sys.argv[1])