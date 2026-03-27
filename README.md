# auto_log_tickets

GitHub Actions automation that runs every 2 hours, collects solved Zendesk tickets, sends formatted ticket contents to Dify for classification, and updates Zendesk ticket forms and custom fields from the returned classification.

## What it does

- Runs on a 2-hour cron schedule from GitHub Actions.
- Supports manual runs through `workflow_dispatch`.
- Uses Zendesk search to find solved tickets for `POWER.fi`, `POWER.no`, `POWER.se`, and `POWER.dk`.
- Builds one text payload per ticket from:
  - the ticket conversation log
  - side conversations
  - side conversation events
- Sends the payload to Dify with `ticket_contents` and `brand`.
- Normalizes Dify outputs like stringified arrays, fenced JSON, and `"FALSE"`.
- Updates Zendesk with the correct `ticket_form_id` and dropdown values.
- Handles the special local-delivery flow where Zendesk requires:
  - a brand-specific delivery form
  - `Delivery & Pick-Up enquiry = Order Status (Shipped)`
  - a brand-specific local carrier field

## Repository layout

- `main.py`: Python automation entrypoint
- `.github/workflows/zendesk-ticket-classifier.yml`: scheduled and manual workflow
- `requirements.txt`: Python dependencies

## Required GitHub Actions secrets

- `ZENDESK_SUBDOMAIN`
- `ZENDESK_EMAIL`
- `ZENDESK_API_TOKEN`
- `DIFY_API_KEY`
- `DIFY_BASE_URL`

Recommended values:

- `ZENDESK_SUBDOMAIN=power1212`
- `ZENDESK_EMAIL=<your Zendesk admin email>`
- `DIFY_BASE_URL=http://dify.power.no`

## Manual workflow inputs

- `brand`: one of `POWER.fi`, `POWER.no`, `POWER.se`, `POWER.dk`
- `ticketids`: comma-separated list such as `4431308,4433148`

If `ticketids` is omitted, the script searches the recent solved-ticket window for the selected brand. If `brand` is omitted too, it processes all four supported brands.

## Local usage

Install dependencies:

```bash
pip install -r requirements.txt
```

Run for a manual list of tickets:

```bash
python main.py --brand POWER.no --ticket-ids 4431308,4433148
```

Run search mode for one brand:

```bash
python main.py --brand POWER.no
```

Run scheduled search mode for all brands:

```bash
python main.py
```

## Notes

- The search window defaults to 2 hours plus a 10-minute overlap to reduce misses caused by search indexing delay.
- Zendesk dropdown fields are updated using the option `value` tags resolved live from Zendesk metadata.
- The delivery shipped flow is applied in two phases to satisfy Zendesk conditional child-field requirements.
