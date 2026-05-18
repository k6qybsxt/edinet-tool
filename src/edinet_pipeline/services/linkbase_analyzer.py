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
XLINK_ARCROLE = f"{{{XLINK_NS}}}arcrole"

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


def _is_public_doc_entry(entry_name: str) -> bool:
    normalized = entry_name.replace("\\", "/")
    return f"/{normalized}".find("/XBRL/PublicDoc/") >= 0


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
            if normalized.endswith(f"/{expected_name}") and _is_public_doc_entry(normalized):
                return entry_name

    for entry_name in entries:
        normalized = entry_name.replace("\\", "/")
        if normalized.endswith(f"_{suffix}.xml") and _is_public_doc_entry(normalized):
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
    labels_by_role = _parse_labels_by_role(root)
    result: dict[str, str] = {}
    for concept_name, role_map in labels_by_role.items():
        resources = [
            (LABEL_ROLE_PRIORITY.get(role, 999), text)
            for role, text in role_map.items()
        ]
        resources.sort(key=lambda item: (item[0], item[1]))
        if resources:
            result[concept_name] = resources[0][1]
    return result


def _parse_labels_by_role(root: etree._Element | None) -> dict[str, dict[str, str]]:
    if root is None:
        return {}

    label_texts: dict[str, list[tuple[int, str, str]]] = {}

    for label_link in root.xpath(".//*[local-name()='labelLink']"):
        concept_by_loc_label: dict[str, str] = {}
        resource_by_label: dict[str, tuple[int, str, str]] = {}

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
                role,
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
    result_by_role: dict[str, dict[str, str]] = {}
    for concept_name, resources in label_texts.items():
        resources.sort(key=lambda item: (item[0], item[1], item[2]))
        for _, role, text in resources:
            result_by_role.setdefault(concept_name, {}).setdefault(role, text)
    return result_by_role


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


def _parse_parent_child_roles(root: etree._Element | None, arc_local_name: str) -> dict[str, set[str]]:
    if root is None:
        return {}

    concept_roles: dict[str, set[str]] = {}
    for link in root.xpath(f".//*[local-name()='{arc_local_name[:-3]}Link']"):
        role = str(link.get(XLINK_ROLE) or "")
        concept_by_loc_label: dict[str, str] = {}

        for loc in link.xpath("./*[local-name()='loc']"):
            loc_label = str(loc.get(XLINK_LABEL) or "")
            href = str(loc.get(XLINK_HREF) or "")
            concept_name = _concept_name_from_href(href)
            if loc_label and concept_name:
                concept_by_loc_label[loc_label] = concept_name
                if role:
                    concept_roles.setdefault(concept_name, set()).add(role)

        for arc in link.xpath(f"./*[local-name()='{arc_local_name}']"):
            for attr_name in (XLINK_FROM, XLINK_TO):
                concept_name = concept_by_loc_label.get(str(arc.get(attr_name) or ""))
                if concept_name and role:
                    concept_roles.setdefault(concept_name, set()).add(role)

    return concept_roles


def _parse_presentation_order(root: etree._Element | None) -> dict[str, dict[str, object]]:
    if root is None:
        return {}

    result: dict[str, dict[str, object]] = {}
    for link_index, link in enumerate(root.xpath(".//*[local-name()='presentationLink']")):
        role = str(link.get(XLINK_ROLE) or "")
        concept_by_loc_label: dict[str, str] = {}
        parent_to_children: dict[str, list[tuple[float, int, str]]] = {}
        parents: set[str] = set()
        children: set[str] = set()

        for loc in link.xpath("./*[local-name()='loc']"):
            loc_label = str(loc.get(XLINK_LABEL) or "")
            href = str(loc.get(XLINK_HREF) or "")
            concept_name = _concept_name_from_href(href)
            if loc_label and concept_name:
                concept_by_loc_label[loc_label] = concept_name

        for arc_index, arc in enumerate(link.xpath("./*[local-name()='presentationArc']")):
            parent = concept_by_loc_label.get(str(arc.get(XLINK_FROM) or ""))
            child = concept_by_loc_label.get(str(arc.get(XLINK_TO) or ""))
            if not parent or not child:
                continue
            order_text = str(arc.get("order") or "")
            try:
                order = float(order_text)
            except ValueError:
                order = 999999.0
            parent_to_children.setdefault(parent, []).append((order, arc_index, child))
            parents.add(parent)
            children.add(child)

        sequence = 0
        visited: set[str] = set()

        def remember(concept_name: str, depth: int) -> None:
            nonlocal sequence
            sequence += 1
            rank = (link_index * 1_000_000) + sequence
            current = result.get(concept_name)
            if current is None or rank < int(current.get("presentation_sequence") or 0):
                result[concept_name] = {
                    "presentation_sequence": rank,
                    "presentation_depth": depth,
                    "presentation_order_role": role,
                }

        def visit(concept_name: str, depth: int) -> None:
            if concept_name in visited:
                return
            visited.add(concept_name)
            remember(concept_name, depth)
            for _order, _arc_index, child in sorted(parent_to_children.get(concept_name, [])):
                visit(child, depth + 1)

        roots = sorted(parents - children)
        for root_concept in roots:
            visit(root_concept, 0)

        for parent in sorted(parents):
            visit(parent, 0)

    return result


def _parse_calculation_relationships(root: etree._Element | None) -> dict[str, list[dict[str, object]]]:
    if root is None:
        return {}

    relationships: dict[str, list[dict[str, object]]] = {}
    for link in root.xpath(".//*[local-name()='calculationLink']"):
        role = str(link.get(XLINK_ROLE) or "")
        concept_by_loc_label: dict[str, str] = {}

        for loc in link.xpath("./*[local-name()='loc']"):
            loc_label = str(loc.get(XLINK_LABEL) or "")
            href = str(loc.get(XLINK_HREF) or "")
            concept_name = _concept_name_from_href(href)
            if loc_label and concept_name:
                concept_by_loc_label[loc_label] = concept_name

        for arc in link.xpath("./*[local-name()='calculationArc']"):
            parent = concept_by_loc_label.get(str(arc.get(XLINK_FROM) or ""))
            child = concept_by_loc_label.get(str(arc.get(XLINK_TO) or ""))
            if not parent or not child:
                continue
            weight_text = str(arc.get("weight") or "")
            try:
                weight: float | str = float(weight_text)
            except ValueError:
                weight = weight_text
            item = {"role": role, "parent": parent, "child": child, "weight": weight}
            relationships.setdefault(parent, []).append(item)
            relationships.setdefault(child, []).append(item)

    return relationships


def _parse_definition_relationships(root: etree._Element | None) -> dict[str, list[dict[str, str]]]:
    if root is None:
        return {}

    relationships: dict[str, list[dict[str, str]]] = {}
    for link in root.xpath(".//*[local-name()='definitionLink']"):
        role = str(link.get(XLINK_ROLE) or "")
        concept_by_loc_label: dict[str, str] = {}

        for loc in link.xpath("./*[local-name()='loc']"):
            loc_label = str(loc.get(XLINK_LABEL) or "")
            href = str(loc.get(XLINK_HREF) or "")
            concept_name = _concept_name_from_href(href)
            if loc_label and concept_name:
                concept_by_loc_label[loc_label] = concept_name

        for arc in link.xpath("./*[local-name()='definitionArc']"):
            parent = concept_by_loc_label.get(str(arc.get(XLINK_FROM) or ""))
            child = concept_by_loc_label.get(str(arc.get(XLINK_TO) or ""))
            if not parent or not child:
                continue
            item = {
                "role": role,
                "arcrole": str(arc.get(XLINK_ARCROLE) or ""),
                "parent": parent,
                "child": child,
            }
            relationships.setdefault(parent, []).append(item)
            relationships.setdefault(child, []).append(item)

    return relationships


def _load_schema_bytes(
    *,
    xbrl_path: str | None,
    zip_path: str | None,
) -> list[bytes]:
    result: list[bytes] = []
    xbrl_file = Path(str(xbrl_path or "")).expanduser() if xbrl_path else None
    zip_file = Path(str(zip_path or "")).expanduser() if zip_path else None

    if xbrl_file:
        adjacent = xbrl_file.with_suffix(".xsd")
        data = _read_bytes(adjacent)
        if data is not None:
            result.append(data)

    if zip_file and zip_file.exists():
        try:
            with ZipFile(zip_file) as zf:
                for entry in zf.infolist():
                    normalized = entry.filename.replace("\\", "/")
                    if entry.is_dir():
                        continue
                    if normalized.endswith(".xsd") and _is_public_doc_entry(normalized):
                        with zf.open(entry.filename) as handle:
                            result.append(handle.read())
        except Exception:
            return result

    return result


def _parse_schema_metadata(schema_roots: list[etree._Element | None]) -> dict[str, dict[str, object]]:
    result: dict[str, dict[str, object]] = {}
    for root in schema_roots:
        if root is None:
            continue
        target_namespace = str(root.get("targetNamespace") or "")
        for element in root.xpath(".//*[local-name()='element']"):
            concept_name = str(element.get("name") or "")
            if not concept_name:
                continue
            result[concept_name] = {
                "target_namespace": target_namespace,
                "type": str(element.get("type") or ""),
                "substitution_group": str(element.get("substitutionGroup") or ""),
                "period_type": str(element.get("{http://www.xbrl.org/2003/instance}periodType") or ""),
                "balance": str(element.get("{http://www.xbrl.org/2003/instance}balance") or ""),
                "abstract": str(element.get("abstract") or ""),
                "nillable": str(element.get("nillable") or ""),
                "id": str(element.get("id") or ""),
            }
    return result


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
    def_root = _parse_xml(
        _load_linkbase_bytes(
            xbrl_path=xbrl_path_text or None,
            zip_path=zip_path_text or None,
            suffix="def",
        )
    )
    schema_roots = [
        _parse_xml(data)
        for data in _load_schema_bytes(
            xbrl_path=xbrl_path_text or None,
            zip_path=zip_path_text or None,
        )
    ]

    labels = _parse_labels(lab_root)
    labels_by_role = _parse_labels_by_role(lab_root)
    pre_children, pre_parents = _parse_parent_child(pre_root, "presentationArc")
    cal_children, cal_parents = _parse_parent_child(cal_root, "calculationArc")
    presentation_roles = _parse_parent_child_roles(pre_root, "presentationArc")
    presentation_orders = _parse_presentation_order(pre_root)
    calculation_roles = _parse_parent_child_roles(cal_root, "calculationArc")
    calculation_relationships = _parse_calculation_relationships(cal_root)
    definition_relationships = _parse_definition_relationships(def_root)
    schema_metadata = _parse_schema_metadata(schema_roots)

    concept_names = set(labels.keys())
    concept_names.update(labels_by_role.keys())
    concept_names.update(pre_children.keys())
    concept_names.update(pre_parents.keys())
    concept_names.update(presentation_orders.keys())
    concept_names.update(cal_children.keys())
    concept_names.update(cal_parents.keys())
    concept_names.update(presentation_roles.keys())
    concept_names.update(calculation_roles.keys())
    concept_names.update(calculation_relationships.keys())
    concept_names.update(definition_relationships.keys())
    concept_names.update(schema_metadata.keys())

    result: dict[str, dict[str, object]] = {}
    for concept_name in concept_names:
        parent_tags = sorted(pre_parents.get(concept_name, set()))
        parent_labels = [labels.get(tag, tag) for tag in parent_tags]
        label = labels.get(concept_name, "")
        calculation_children_count = len(cal_children.get(concept_name, set()))
        is_total = calculation_children_count > 0 or "合計" in label

        result[concept_name] = {
            "label": label,
            "labels_by_role": labels_by_role.get(concept_name, {}),
            "presentation_roles": sorted(presentation_roles.get(concept_name, set())),
            **presentation_orders.get(concept_name, {}),
            "presentation_parent_tags": parent_tags,
            "presentation_parent_labels": parent_labels,
            "presentation_child_count": len(pre_children.get(concept_name, set())),
            "calculation_roles": sorted(calculation_roles.get(concept_name, set())),
            "calculation_parent_tags": sorted(cal_parents.get(concept_name, set())),
            "calculation_children_count": calculation_children_count,
            "calculation_relationships": calculation_relationships.get(concept_name, []),
            "definition_roles": sorted({
                str(item.get("role") or "")
                for item in definition_relationships.get(concept_name, [])
                if item.get("role")
            }),
            "definition_relationships": definition_relationships.get(concept_name, []),
            "schema": schema_metadata.get(concept_name, {}),
            "is_total": is_total,
        }

    return result


def analyze_linkbase_structure(
    *,
    xbrl_path: str | None = None,
    zip_path: str | None = None,
) -> dict[str, dict[str, object]]:
    return _analyze_cached(str(xbrl_path or ""), str(zip_path or ""))
