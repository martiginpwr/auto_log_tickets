import argparse
import ast
import html
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional

import requests


DEFAULT_BRANDS = ("POWER.fi", "POWER.no", "POWER.se", "POWER.dk")

BRAND_CONFIG = {
    "POWER.fi": {
        "delivery_form_name": "Delivery: Delivery & PickUp - FI",
        "delivery_child_field_title": "Delivery enquiry - FI - Local Delivery",
    },
    "POWER.no": {
        "delivery_form_name": "Delivery: Delivery & PickUp - NO",
        "delivery_child_field_title": "Delivery enquiry - NO - Local Delivery",
    },
    "POWER.se": {
        "delivery_form_name": "Delivery: Delivery & PickUp - SE",
        "delivery_child_field_title": "Delivery enquiry - SE - Local Delivery",
    },
    "POWER.dk": {
        "delivery_form_name": "Delivery: Delivery & PickUp - DK",
        "delivery_child_field_title": "Delivery enquiry - DK - Local Delivery",
    },
}

PRIMARY_CATEGORY_CONFIG = {
    "Aftersales: Service and Repair": {
        "form_name": "Aftersales: Service and Repair",
        "field_title": "Service & Repair enquiry",
    },
    "Cancel & Return enquiry": {
        "form_name": "Delivery: Cancel & Return",
        "field_title": "Cancel & Return enquiry",
    },
    "Delivery: Delivery & PickUp": {
        "special": "delivery",
        "field_title": "Delivery & Pick-Up enquiry",
    },
    "MyPOWER enquiry": {
        "form_name": "MyPOWER",
        "field_title": "MyPOWER enquiry",
    },
    "Order & Product info enquiry": {
        "form_name": "Others: Order & Product info",
        "field_title": "Order & Product info enquiry",
    },
    "Service & Repair enquiry": {
        "form_name": "Aftersales: Service and Repair",
        "field_title": "Service & Repair enquiry",
    },
    "Type of B2B enquiry": {
        "form_name": "B2B",
        "field_title": "Type of B2B enquiry",
    },
    "Type of Eletra enquiry": {
        "form_name": "Eletra",
        "field_title": "Type of Eletra enquiry",
    },
    "Type of VIP enquiry": {
        "form_name": "VIP",
        "field_title": "Type of VIP enquiry",
    },
}

DELIVERY_PARENT_FIELD_TITLE = "Delivery & Pick-Up enquiry"


class HtmlToTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def get_text(self) -> str:
        return "".join(self.parts)


def html_to_text(value: str) -> str:
    parser = HtmlToTextParser()
    parser.feed(value)
    parser.close()
    text = html.unescape(parser.get_text())
    text = text.replace("\r", "")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_label(value: str) -> str:
    text = html.unescape(value or "")
    text = text.strip().upper()
    text = text.replace("&", " AND ")
    text = re.sub(r"\(SHIPPED\)", "", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_dify_payload(raw: Any) -> Any:
    if raw is None:
        return None
    if raw is False:
        return None
    if isinstance(raw, list):
        return [str(item).strip() for item in raw]
    if isinstance(raw, dict):
        return raw

    text = str(raw).strip()
    if not text:
        return None

    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip().strip("`").strip()

    if text.upper() == "FALSE":
        return None

    for parser in (json.loads, ast.literal_eval):
        try:
            parsed = parser(text)
            if parsed is False or parsed is None:
                return None
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed]
            if isinstance(parsed, str) and parsed.strip().upper() == "FALSE":
                return None
            return parsed
        except Exception:
            continue

    return text


def ensure_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value]
    return [str(value).strip()]


def parse_ticket_ids(raw: str) -> List[int]:
    ids: List[int] = []
    for part in (raw or "").split(","):
        token = part.strip()
        if not token:
            continue
        ids.append(int(token))
    return ids


def isoformat_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass
class TicketField:
    id: int
    title: str
    options: List[Dict[str, Any]]


@dataclass
class TicketForm:
    id: int
    name: str
    restricted_brand_ids: List[int]
    ticket_field_ids: List[int]
    agent_conditions: List[Dict[str, Any]]


@dataclass
class UpdatePhase:
    ticket_form_id: Optional[int]
    custom_fields: List[Dict[str, Any]]
    description: str


@dataclass
class UpdateInstruction:
    phases: List[UpdatePhase]
    summary: str


class ZendeskClient:
    def __init__(self, subdomain: str, email: str, api_token: str) -> None:
        self.base_url = f"https://{subdomain}.zendesk.com/api/v2"
        self.session = requests.Session()
        self.session.auth = (f"{email}/token", api_token)
        self.session.headers.update({"Content-Type": "application/json"})

    def _request(self, method: str, url: str, **kwargs: Any) -> Dict[str, Any]:
        response = self.session.request(method, url, timeout=60, **kwargs)
        response.raise_for_status()
        if response.text:
            return response.json()
        return {}

    def _paginate(
        self, endpoint: str, key: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        url = f"{self.base_url}{endpoint}"
        current_params = dict(params or {})
        items: List[Dict[str, Any]] = []

        while url:
            payload = self._request("GET", url, params=current_params)
            data = payload.get(key, [])
            if isinstance(data, list):
                items.extend(data)
            elif data:
                items.append(data)

            next_url = payload.get("next_page")
            if not next_url:
                links = payload.get("links") or {}
                next_url = links.get("next")

            url = next_url
            current_params = {}

        return items

    def get_brands(self) -> List[Dict[str, Any]]:
        return self._paginate("/brands.json", "brands")

    def get_ticket_forms(self) -> List[Dict[str, Any]]:
        return self._paginate("/ticket_forms.json", "ticket_forms", {"active": "true"})

    def get_ticket_fields(self) -> List[Dict[str, Any]]:
        return self._paginate("/ticket_fields.json", "ticket_fields")

    def search_tickets(
        self, brand: str, start_at: datetime, end_at: datetime
    ) -> List[Dict[str, Any]]:
        query = (
            f'type:ticket brand:"{brand}" status:solved '
            f"solved>{isoformat_z(start_at)} solved<={isoformat_z(end_at)}"
        )
        return self._paginate(
            "/search.json",
            "results",
            {
                "query": query,
                "sort_by": "updated_at",
                "sort_order": "desc",
                "per_page": 100,
            },
        )

    def get_ticket(self, ticket_id: int) -> Dict[str, Any]:
        payload = self._request("GET", f"{self.base_url}/tickets/{ticket_id}.json")
        return payload["ticket"]

    def get_conversation_log(self, ticket_id: int) -> List[Dict[str, Any]]:
        return self._paginate(
            f"/tickets/{ticket_id}/conversation_log.json", "conversation_log"
        )

    def get_side_conversations(self, ticket_id: int) -> List[Dict[str, Any]]:
        return self._paginate(
            f"/tickets/{ticket_id}/side_conversations.json", "side_conversations"
        )

    def get_side_conversation_events(
        self, ticket_id: int, side_conversation_id: str
    ) -> List[Dict[str, Any]]:
        return self._paginate(
            f"/tickets/{ticket_id}/side_conversations/{side_conversation_id}/events.json",
            "events",
        )

    def update_ticket(
        self,
        ticket_id: int,
        updated_stamp: str,
        custom_fields: List[Dict[str, Any]],
        ticket_form_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        ticket_payload: Dict[str, Any] = {
            "updated_stamp": updated_stamp,
            "safe_update": True,
            "custom_fields": custom_fields,
        }
        if ticket_form_id is not None:
            ticket_payload["ticket_form_id"] = ticket_form_id

        payload = self._request(
            "PUT",
            f"{self.base_url}/tickets/{ticket_id}.json",
            data=json.dumps({"ticket": ticket_payload}),
        )
        return payload["ticket"]


class DifyClient:
    def __init__(self, base_url: str, api_key: str, user: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.user = user
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        )

    def classify(self, ticket_contents: str, brand: str, ticket_id: int) -> Dict[str, Any]:
        payload = {
            "inputs": {"ticket_contents": ticket_contents, "brand": brand},
            "response_mode": "blocking",
            "user": f"{self.user}:{ticket_id}",
        }
        response = self.session.post(
            f"{self.base_url}/v1/workflows/run",
            data=json.dumps(payload),
            timeout=180,
        )
        response.raise_for_status()
        body = response.json()
        data = body.get("data") or {}
        status = data.get("status")
        if status != "succeeded":
            raise RuntimeError(f"Dify classification failed for ticket {ticket_id}: {body}")
        return data.get("outputs") or {}


class MetadataCatalog:
    def __init__(self, zendesk: ZendeskClient) -> None:
        self.brands = zendesk.get_brands()
        self.forms = [
            TicketForm(
                id=int(item["id"]),
                name=item["name"],
                restricted_brand_ids=item.get("restricted_brand_ids", []),
                ticket_field_ids=item.get("ticket_field_ids", []),
                agent_conditions=item.get("agent_conditions", []),
            )
            for item in zendesk.get_ticket_forms()
        ]
        self.fields = [
            TicketField(
                id=int(item["id"]),
                title=item["title"],
                options=item.get("custom_field_options") or [],
            )
            for item in zendesk.get_ticket_fields()
        ]
        self.brand_name_by_id = {int(item["id"]): item["name"] for item in self.brands}
        self.form_by_name = {item.name: item for item in self.forms}
        self.field_by_title = {item.title: item for item in self.fields}

    def form(self, name: str) -> TicketForm:
        try:
            return self.form_by_name[name]
        except KeyError as exc:
            raise KeyError(f"Ticket form not found: {name}") from exc

    def field(self, title: str) -> TicketField:
        try:
            return self.field_by_title[title]
        except KeyError as exc:
            raise KeyError(f"Ticket field not found: {title}") from exc

    def brand_name(self, brand_id: Optional[int]) -> Optional[str]:
        if brand_id is None:
            return None
        return self.brand_name_by_id.get(int(brand_id))

    def option_value(self, field_title: str, desired_name: str) -> str:
        field = self.field(field_title)
        wanted = normalize_label(desired_name)
        candidates: List[Dict[str, Any]] = []

        for option in field.options:
            names = {
                normalize_label(str(option.get("name", ""))),
                normalize_label(str(option.get("raw_name", ""))),
                normalize_label(str(option.get("value", ""))),
            }
            stripped_names = {name.replace(" SHIPPED", "").strip() for name in names}
            if wanted in names or wanted in stripped_names:
                return str(option["value"])
            if any(wanted and wanted in name for name in names | stripped_names):
                candidates.append(option)

        if len(candidates) == 1:
            return str(candidates[0]["value"])

        raise KeyError(
            f'Option "{desired_name}" not found for field "{field_title}". '
            f"Available options: {[option.get('name') for option in field.options]}"
        )


def extract_text(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload.strip()
    if isinstance(payload, dict):
        for key in ("body", "html_body", "preview_text", "text"):
            if key in payload and payload[key]:
                value = str(payload[key])
                return html_to_text(value) if "<" in value else value.strip()
        if payload.get("type") == "html" and payload.get("body"):
            return html_to_text(str(payload["body"]))
    return str(payload).strip()


def format_conversation_entry(event: Dict[str, Any]) -> str:
    timestamp = event.get("created_at") or event.get("received_at") or "unknown-time"
    event_type = event.get("type", "unknown")
    author = event.get("author") or {}
    author_name = author.get("display_name") or author.get("name") or author.get("type") or "unknown-author"
    metadata = event.get("metadata") or {}
    visibility = ""
    if event_type == "Comment":
        visibility = "private" if metadata.get("public") is False else "public"

    body = extract_text(event.get("content"))
    if not body and event.get("attachments"):
        body = "[Attachment only]"
    descriptor = f"[Conversation Log] {timestamp} | {event_type} | author={author_name}"
    if visibility:
        descriptor += f" | visibility={visibility}"
    return f"{descriptor}\n{body or '[No text content]'}"


def format_side_conversation_event(event: Dict[str, Any], side_conversation: Dict[str, Any]) -> str:
    timestamp = event.get("created_at") or "unknown-time"
    event_type = event.get("type", "unknown")
    actor = event.get("actor") or {}
    actor_name = actor.get("name") or actor.get("email") or "unknown-actor"
    message = event.get("message") or {}
    body = extract_text(message)
    if not body and event.get("updates"):
        body = json.dumps(event["updates"], ensure_ascii=True, sort_keys=True)
    subject = side_conversation.get("subject") or side_conversation.get("id") or "unknown-side-conversation"
    return (
        f"[Side Conversation Log] {timestamp} | subject={subject} | type={event_type} | actor={actor_name}\n"
        f"{body or '[No text content]'}"
    )


def build_ticket_contents(
    ticket: Dict[str, Any],
    brand: str,
    conversation_log: List[Dict[str, Any]],
    side_conversations: List[Dict[str, Any]],
    side_conversation_events: Dict[str, List[Dict[str, Any]]],
) -> str:
    lines = [
        f"Ticket ID: {ticket['id']}",
        f"Brand: {brand}",
        f"Subject: {ticket.get('subject') or ''}",
        "",
        "=== Conversation Log ===",
    ]

    if conversation_log:
        for entry in conversation_log:
            lines.append(format_conversation_entry(entry))
            lines.append("")
    else:
        lines.append("[No conversation log entries]")
        lines.append("")

    lines.append("=== Side Conversation Log ===")
    if side_conversations:
        for side_conversation in side_conversations:
            sc_id = str(side_conversation["id"])
            events = side_conversation_events.get(sc_id, [])
            if events:
                for event in events:
                    lines.append(format_side_conversation_event(event, side_conversation))
                    lines.append("")
            else:
                subject = side_conversation.get("subject") or sc_id
                lines.append(f"[Side Conversation Log] subject={subject}\n[No side conversation events]")
                lines.append("")
    else:
        lines.append("[No side conversations]")

    return "\n".join(lines).strip()


def resolve_brand_for_ticket(
    requested_brand: Optional[str], actual_brand_name: Optional[str]
) -> Optional[str]:
    if actual_brand_name in BRAND_CONFIG:
        return actual_brand_name
    if requested_brand in BRAND_CONFIG:
        return requested_brand
    return requested_brand or actual_brand_name


def build_update_instruction(
    brand: str, outputs: Dict[str, Any], metadata: MetadataCatalog
) -> Optional[UpdateInstruction]:
    primary = clean_dify_payload(outputs.get("primary_reason"))
    secondary_present = "secondary_reason" in outputs
    secondary = clean_dify_payload(outputs.get("secondary_reason"))

    if primary is None:
        return None

    primary_list = ensure_list(primary)
    if len(primary_list) < 2:
        raise ValueError(f"Invalid primary_reason payload: {outputs.get('primary_reason')}")

    main_category = primary_list[0]
    sub_category = primary_list[1]

    if secondary_present:
        if secondary is None:
            logging.info("secondary_reason was FALSE; skipping unresolved shipped delivery path.")
            return None
        secondary_list = ensure_list(secondary)
        if not secondary_list:
            return None
        if main_category != "Delivery: Delivery & PickUp":
            raise ValueError(
                "secondary_reason was provided but primary_reason was not the delivery flow."
            )
        brand_details = BRAND_CONFIG[brand]
        form = metadata.form(brand_details["delivery_form_name"])
        parent_field_title = DELIVERY_PARENT_FIELD_TITLE
        child_field_title = brand_details["delivery_child_field_title"]
        parent_value = metadata.option_value(parent_field_title, sub_category)
        child_value = metadata.option_value(child_field_title, secondary_list[0])
        return UpdateInstruction(
            phases=[
                UpdatePhase(
                    ticket_form_id=form.id,
                    custom_fields=[{"id": metadata.field(parent_field_title).id, "value": parent_value}],
                    description="set delivery form and parent delivery reason",
                ),
                UpdatePhase(
                    ticket_form_id=None,
                    custom_fields=[{"id": metadata.field(child_field_title).id, "value": child_value}],
                    description="set local delivery carrier",
                ),
            ],
            summary=(
                f'{form.name} -> {parent_field_title}="{sub_category}" '
                f'-> {child_field_title}="{secondary_list[0]}"'
            ),
        )

    category_config = PRIMARY_CATEGORY_CONFIG.get(main_category)
    if not category_config:
        raise KeyError(f"Unsupported primary category from Dify: {main_category}")

    if category_config.get("special") == "delivery":
        if brand not in BRAND_CONFIG:
            raise KeyError(f"Delivery flow is unsupported for brand: {brand}")
        if normalize_label(sub_category) == normalize_label("Order Status (Shipped)"):
            logging.info(
                "Received shipped delivery without secondary_reason; skipping because local carrier is required."
            )
            return None
        form = metadata.form(BRAND_CONFIG[brand]["delivery_form_name"])
        field_title = category_config["field_title"]
    else:
        form = metadata.form(category_config["form_name"])
        field_title = category_config["field_title"]

    field = metadata.field(field_title)
    field_value = metadata.option_value(field_title, sub_category)
    return UpdateInstruction(
        phases=[
            UpdatePhase(
                ticket_form_id=form.id,
                custom_fields=[{"id": field.id, "value": field_value}],
                description=f'set form "{form.name}" and field "{field_title}"',
            )
        ],
        summary=f'{form.name} -> {field_title}="{sub_category}"',
    )


def process_ticket(
    ticket_id: int,
    requested_brand: Optional[str],
    zendesk: ZendeskClient,
    dify: DifyClient,
    metadata: MetadataCatalog,
) -> None:
    ticket = zendesk.get_ticket(ticket_id)
    actual_brand = metadata.brand_name(ticket.get("brand_id"))
    brand = resolve_brand_for_ticket(requested_brand, actual_brand)
    if not brand:
        logging.warning("Skipping ticket %s because brand could not be resolved.", ticket_id)
        return

    conversation_log = zendesk.get_conversation_log(ticket_id)
    side_conversations = zendesk.get_side_conversations(ticket_id)
    side_conversation_events: Dict[str, List[Dict[str, Any]]] = {}
    for side_conversation in side_conversations:
        sc_id = str(side_conversation["id"])
        side_conversation_events[sc_id] = zendesk.get_side_conversation_events(ticket_id, sc_id)

    ticket_contents = build_ticket_contents(
        ticket=ticket,
        brand=brand,
        conversation_log=conversation_log,
        side_conversations=side_conversations,
        side_conversation_events=side_conversation_events,
    )
    outputs = dify.classify(ticket_contents=ticket_contents, brand=brand, ticket_id=ticket_id)
    logging.info("Ticket %s Dify outputs: %s", ticket_id, json.dumps(outputs, ensure_ascii=True))

    instruction = build_update_instruction(brand=brand, outputs=outputs, metadata=metadata)
    if not instruction:
        logging.info("Ticket %s: no ticket update required.", ticket_id)
        return

    logging.info("Ticket %s update plan: %s", ticket_id, instruction.summary)
    updated_stamp = ticket["updated_at"]
    for phase in instruction.phases:
        updated_ticket = zendesk.update_ticket(
            ticket_id=ticket_id,
            updated_stamp=updated_stamp,
            custom_fields=phase.custom_fields,
            ticket_form_id=phase.ticket_form_id,
        )
        updated_stamp = updated_ticket["updated_at"]
        logging.info("Ticket %s: completed phase: %s", ticket_id, phase.description)


def collect_ticket_ids_for_brand(
    brand: str,
    zendesk: ZendeskClient,
    start_at: datetime,
    end_at: datetime,
) -> List[int]:
    results = zendesk.search_tickets(brand=brand, start_at=start_at, end_at=end_at)
    ticket_ids = sorted({int(item["id"]) for item in results})
    logging.info(
        "Brand %s: found %s solved tickets in window %s -> %s",
        brand,
        len(ticket_ids),
        isoformat_z(start_at),
        isoformat_z(end_at),
    )
    return ticket_ids


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Zendesk ticket contents, classify them in Dify, and update Zendesk fields."
    )
    parser.add_argument("--brand", help="Single brand to process, for example POWER.no")
    parser.add_argument(
        "--ticket-ids",
        help="Comma-separated Zendesk ticket ids. When omitted, the script searches solved tickets in the time window.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=int(os.getenv("SEARCH_WINDOW_HOURS", "2")),
        help="Search window in hours for scheduled discovery mode.",
    )
    parser.add_argument(
        "--overlap-minutes",
        type=int,
        default=int(os.getenv("SEARCH_OVERLAP_MINUTES", "10")),
        help="Overlap added to the search window to reduce search indexing misses.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LOG_LEVEL", "INFO"),
        help="Python logging level.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    zendesk = ZendeskClient(
        subdomain=env_required("ZENDESK_SUBDOMAIN"),
        email=env_required("ZENDESK_EMAIL"),
        api_token=env_required("ZENDESK_API_TOKEN"),
    )
    dify = DifyClient(
        base_url=os.getenv("DIFY_BASE_URL") or "http://dify.power.no",
        api_key=env_required("DIFY_API_KEY"),
        user=os.getenv("DIFY_USER", "github-actions"),
    )
    metadata = MetadataCatalog(zendesk)

    if args.brand and args.brand not in BRAND_CONFIG:
        raise ValueError(
            f"Unsupported brand '{args.brand}'. Supported brands: {', '.join(DEFAULT_BRANDS)}"
        )

    requested_ticket_ids = parse_ticket_ids(args.ticket_ids) if args.ticket_ids else []

    failures: List[str] = []

    if requested_ticket_ids:
        for ticket_id in requested_ticket_ids:
            try:
                process_ticket(
                    ticket_id=ticket_id,
                    requested_brand=args.brand,
                    zendesk=zendesk,
                    dify=dify,
                    metadata=metadata,
                )
            except Exception as exc:
                logging.exception("Ticket %s failed: %s", ticket_id, exc)
                failures.append(f"ticket {ticket_id}: {exc}")
        if failures:
            raise RuntimeError(f"One or more tickets failed: {'; '.join(failures)}")
        return

    brands = [args.brand] if args.brand else list(DEFAULT_BRANDS)
    end_at = datetime.now(timezone.utc)
    start_at = end_at - timedelta(hours=args.window_hours, minutes=args.overlap_minutes)

    for brand in brands:
        ticket_ids = collect_ticket_ids_for_brand(
            brand=brand,
            zendesk=zendesk,
            start_at=start_at,
            end_at=end_at,
        )
        for ticket_id in ticket_ids:
            try:
                process_ticket(
                    ticket_id=ticket_id,
                    requested_brand=brand,
                    zendesk=zendesk,
                    dify=dify,
                    metadata=metadata,
                )
            except Exception as exc:
                logging.exception("Ticket %s failed: %s", ticket_id, exc)
                failures.append(f"ticket {ticket_id}: {exc}")

    if failures:
        raise RuntimeError(f"One or more tickets failed: {'; '.join(failures)}")


if __name__ == "__main__":
    main()
