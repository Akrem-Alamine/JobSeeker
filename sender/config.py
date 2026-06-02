import json
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "config.json"

DEFAULTS = {
    "your_name":                 "",
    "your_email":                "",
    "gmail_app_password":        "",
    "your_university":           "",
    "your_graduation":           "",
    "your_internship_company":   "Qleap Networks",
    "template_subject":          "DevOps / Cloud Engineer — {{company}}",
    "template_body": (
        "Hi {{greeting}},\n\n"
        "My name is {{your_name}} and I am a computer engineering student at {{your_university}} "
        "finishing my end of study internship at {{your_internship_company}}. "
        "I am graduating in {{your_graduation}} and looking for my first position in DevOps, "
        "cloud or system administration and I came across {{company}} which I think would be a great fit "
        "to start my career. If you have a few minutes for a quick conversation I would really appreciate it.\n\n"
        "Best regards\n"
        "{{your_name}}\n"
        "{{your_email}}"
    ),
    "last_batch_at": None,
    "batch_size":    1500,
}


def load() -> dict:
    if CONFIG_PATH.exists():
        stored = json.loads(CONFIG_PATH.read_text())
        return {**DEFAULTS, **stored}
    return dict(DEFAULTS)


def save(data: dict):
    CONFIG_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False))
