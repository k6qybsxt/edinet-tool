from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from zipfile import ZipFile

from lxml import etree


XLINK_NS = "http://www.w3.org/1999/xlink"
XLINK_HREF = f"{{{XLINK_NS}}}href"
XLINK_LABEL = f"{{{XLINK_NS}}}label"
XLINK_FROM = f"{{{XLINK_NS}}}from"
XLINK_TO = f"{{{XLINK_NS}}}to"
XLINK_ROLE = f"{{{XLINK_NS}}}role"

LABEL_ROLE_PRIORITY = {
    "http://www.xbrl.org/2003/role/label": 0,
    "http://disclosure.edinet-fsa.go.jp/jppfs/sec/role/totalLabel": 1,
    "http://www.xbrl.org/2003/role/terseLabel": 2,
    "http://www.xbrl.org/2003/role/verboseLabel": 3,
}


def _concept_name_from_href(href: str) -> str:
    fragment = str(href or "").split("#", 1)[-1]
    if not fragment:
        return ""
    if "_" in fragment:
        return fragment.rsplit("_", 1)[-1]
    return fragment


def _read_bytes(path: Path) -> bytes | None:
    if not path.exists():
        return None
    try:
        return path.read_bytes()
    except Exception:
        return None


def _find_public_doc_companion_entry_name(
    *,
    zip_path: Path,
    xbrl_stem: str | None,
    suffix: str,
) -> str | None:
    try:
        with ZipFile(zip_path) as zf:
            entries = [entry.filename for entry in zf.infolist() if not entry.is_dir()]
    except Exception:
        return None

    if xbrl_stem:
        expected_name = f"{xbrl_stem}_{suffix}.xml"
        for entry_name in entries:
            normalized = entry_name.replace("\\", "/")
            if normalized.endswith(f"/{expected_name}") and "/XBRL/PublicDoc/" in normalized:
                return entry_name

    for entry_name in entries:
        normalized = entry_name.replace("\\", "/")
        if normalized.endswith(f"_{suffix}.xml") and "/XBRL/PublicDoc/" in normalized:
            return entry_name

    return None


def _load_linkbase_bytes(
    *,
    xbrl_path: str | None,
    zip_path: str | None,
    suffix: str,
) -> bytes | None:
    xbrl_file = Path(str(xbrl_path or "")).expanduser() if xbrl_path else None
    zip_file = Path(str(zip_path or "")).expanduser() if zip_path else None
    xbrl_stem = xbrl_file.stem if xbrl_file and xbrl_file.name else None

    if xbrl_file:
        adjacent = xbrl_file.with_name(f"{xbrl_file.stem}_{suffix}.xml")
        data = _read_bytes(adjacent)
        if data is not None:
            return data

    if zip_file and zip_file.exists():
        entry_name = _find_public_doc_companion_entry_name(
            zip_path=zip_file,
            xbrl_stem=xbrl_stem,
            suffix=suffix,
        )
        if not entry_name:
            return None
        try:
            with ZipFile(zip_file) as zf:
                with zf.open(entry_name) as handle:
                    return handle.read()
        except Exception:
            return None

    return None


def _parse_xml(data: bytes | None) -> etree._Element | None:
    if not data:
        return None
    try:
        return etree.fromstring(data)
    except Exception:
        return None


def _parse_labels(root: etree._Element | None) -> dict[str, str]:
    if root is None:
        return {}

    label_texts: dict[str, list[tuple[int, str]]] = {}

    for label_link in root.xpath(".//*[local-name()='labelLink']"):
        concept_by_loc_label: dict[str, str] = {}
        resource_by_label: dict[str, tuple[int, str]] = {}

        for loc in label_link.xpath("./*[local-name()='loc']"):
            loc_label = str(loc.get(XLINK_LABEL) or "")
            href = str(loc.get(XLINK_HREF) or "")
            concept_name = _concept_name_from_href(href)
            if loc_label and concept_name:
                concept_by_loc_label[loc_label] = concept_name

        for resource in label_link.xpath("./*[local-name()='label']"):
            resource_label = str(resource.get(XLINK_LABEL) or "")
            role = str(resource.get(XLINK_ROLE) or "")
            text = "".join(resource.itertext()).strip()
            if not resource_label or not text:
                continue
            resource_by_label[resource_label] = (
                LABEL_ROLE_PRIORITY.get(role, 999),
                text,
            )

        for arc in label_link.xpath("./*[local-name()='labelArc']"):
            from_label = str(arc.get(XLINK_FROM) or "")
            to_label = str(arc.get(XLINK_TO) or "")
            concept_name = concept_by_loc_label.get(from_label)
            resource = resource_by_label.get(to_label)
            if not concept_name or not resource:
                continue
            label_texts.setdefault(concept_name, []).append(resource)

    result: dict[str, str] = {}
    for concept_name, resources in label_texts.items():
        resources.sort(key=lambda item: (item[0], item[1]))
        result[concept_name] = resources[0][1]
    return result


def _parse_parent_child(root: etree._Element | None, arc_local_name: str) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    if root is None:
        return {}, {}

    parent_to_children: dict[str, set[str]] = {}
    child_to_parents: dict[str, set[str]] = {}

    for link in root.xpath(f".//*[local-name()='{arc_local_name[:-3]}Link']"):
        concept_by_loc_label: dict[str, str] = {}

        for loc in link.xpath("./*[local-name()='loc']"):
            loc_label = str(loc.get(XLINK_LABEL) or "")
            href = str(loc.get(XLINK_HREF) or "")
            concept_name = _concept_name_from_href(href)
            if loc_label and concept_name:
                concept_by_loc_label[loc_label] = concept_name

        for arc in link.xpath(f"./*[local-name()='{arc_local_name}']"):
            from_label = str(arc.get(XLINK_FROM) or "")
            to_label = str(arc.get(XLINK_TO) or "")
            parent = concept_by_loc_label.get(from_label)
            child = concept_by_loc_label.get(to_label)
            if not parent or not child:
                continue
            parent_to_children.setdefault(parent, set()).add(child)
            child_to_parents.setdefault(child, set()).add(parent)

    return parent_to_children, child_to_parents


@lru_cache(maxsize=256)
def _analyze_cached(xbrl_path_text: str, zip_path_text: str) -> dict[str, dict[str, object]]:
    pre_root = _parse_xml(
        _load_linkbase_bytes(
            xbrl_path=xbrl_path_text or None,
            zip_path=zip_path_text or None,
            suffix="pre",
        )
    )
    cal_root = _parse_xml(
        _load_linkbase_bytes(
            xbrl_path=xbrl_path_text or None,
            zip_path=zip_path_text or None,
            suffix="cal",
        )
    )
    lab_root = _parse_xml(
        _load_linkbase_bytes(
            xbrl_path=xbrl_path_text or None,
            zip_path=zip_path_text or None,
            suffix="lab",
        )
    )

    labels = _parse_labels(lab_root)
    pre_children, pre_parents = _parse_parent_child(pre_root, "presentationArc")
    cal_children, cal_parents = _parse_parent_child(cal_root, "calculationArc")

    concept_names = set(labels.keys())
    concept_names.update(pre_children.keys())
    concept_names.update(pre_parents.keys())
    concept_names.update(cal_children.keys())
    concept_names.update(cal_parents.keys())

    result: dict[str, dict[str, object]] = {}
    for concept_name in concept_names:
        parent_tags = sorted(pre_parents.get(concept_name, set()))
        parent_labels = [labels.get(tag, tag) for tag in parent_tags]
        label = labels.get(concept_name, "")
        calculation_children_count = len(cal_children.get(concept_name, set()))
        is_total = calculation_children_count > 0 or "合計" in label

        result[concept_name] = {
            "label": label,
            "presentation_parent_tags": parent_tags,
            "presentation_parent_labels": parent_labels,
            "presentation_child_count": len(pre_children.get(concept_name, set())),
            "calculation_parent_tags": sorted(cal_parents.get(concept_name, set())),
            "calculation_children_count": calculation_children_count,
            "is_total": is_total,
        }

    return result


def analyze_linkbase_structure(
    *,
    xbrl_path: str | None = None,
    zip_path: str | None = None,
) -> dict[str, dict[str, object]]:
    return _analyze_cached(str(xbrl_path or ""), str(zip_path or ""))
