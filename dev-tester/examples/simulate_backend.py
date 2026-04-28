#!/usr/bin/env python3
"""Tiny RustZap consumer simulator for local development.

It behaves like another backend: it knows project/company ids, calls the real
RustZap HTTP API, leases dirty conversations, reads messages/media, and acks the
processed sequence.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


class RustZapClient:
    def __init__(self) -> None:
        self.base_url = env("RUSTZAP_BASE_URL", "http://127.0.0.1:8167").rstrip("/")
        self.project_id = env("RUSTZAP_PROJECT_ID", "tetoz")
        self.company_id = env("RUSTZAP_COMPANY_ID", "company_dev")
        self.channel_id = env("RUSTZAP_CHANNEL_ID", "ch_dev_whatsapp")
        self.consumer_id = env("RUSTZAP_CONSUMER_ID", "python_example_backend")
        self.api_key = env("RUSTZAP_PROJECT_API_KEY", "dev_project_key")

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        data = None if body is None else json.dumps(body).encode("utf-8")
        req_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        if body is not None:
            req_headers["Content-Type"] = "application/json"
        if headers:
            req_headers.update(headers)
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=req_headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = response.read().decode("utf-8")
                if not payload:
                    return None
                content_type = response.headers.get("content-type", "")
                return json.loads(payload) if "application/json" in content_type else payload
        except urllib.error.HTTPError as exc:
            payload = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {path} -> {exc.code}: {payload}") from exc

    def tenant_path(self, suffix: str) -> str:
        return f"/v1/projects/{self.project_id}/companies/{self.company_id}{suffix}"

    def ensure_tenant(self) -> None:
        self.request("POST", "/v1/projects", {"id": self.project_id, "name": "RustZap Python Example"})
        self.request(
            "POST",
            f"/v1/projects/{self.project_id}/companies",
            {
                "id": self.company_id,
                "external_company_id": self.company_id,
                "name": "Python Example Company",
            },
        )
        self.request(
            "POST",
            self.tenant_path("/channels/whatsapp/accounts"),
            {"id": self.channel_id, "label": "Python Example", "phone_e164": None},
        )

    def seed(self) -> None:
        direct_conversation = env("RUSTZAP_EXAMPLE_CONVERSATION", "5511999999999@s.whatsapp.net")
        direct_profile_url = env("RUSTZAP_EXAMPLE_PROFILE_PICTURE_URL", "").strip()
        group_member_profile_url = env("RUSTZAP_EXAMPLE_GROUP_MEMBER_PROFILE_PICTURE_URL", "").strip()
        direct_text = {
            "conversation_id": direct_conversation,
            "channel_id": self.channel_id,
            "from_phone_e164": "+5511999999999",
            "sender_name": "Cliente RustZap",
            "text": "Mensagem simulada pelo backend Python",
        }
        direct_image = {
            "conversation_id": direct_conversation,
            "channel_id": self.channel_id,
            "from_phone_e164": "+5511999999999",
            "sender_name": "Cliente RustZap",
            "media_type": "image",
            "mime_type": "image/svg+xml",
            "filename": "rustyzap-example.svg",
            "caption": "Foto simulada com metadados R2",
            "size_bytes": 8192,
        }
        group_message = {
            "group_id": env("RUSTZAP_EXAMPLE_GROUP", "120363000000000000@g.us"),
            "channel_id": self.channel_id,
            "from_phone_e164": "+5511888888888",
            "sender_name": "Pessoa do Grupo",
            "text": "Mensagem simulada em grupo",
        }
        if direct_profile_url:
            direct_text["profile_picture_url"] = direct_profile_url
            direct_image["profile_picture_url"] = direct_profile_url
        if group_member_profile_url:
            group_message["profile_picture_url"] = group_member_profile_url
        self.request(
            "POST",
            f"/v1/dev/projects/{self.project_id}/companies/{self.company_id}/simulate/inbound-text",
            direct_text,
        )
        self.request(
            "POST",
            f"/v1/dev/projects/{self.project_id}/companies/{self.company_id}/simulate/inbound-image",
            direct_image,
        )
        self.request(
            "POST",
            f"/v1/dev/projects/{self.project_id}/companies/{self.company_id}/simulate/group-event",
            group_message,
        )

    def poll_once(self) -> int:
        query = urllib.parse.urlencode({"consumer_id": self.consumer_id, "limit": "20"})
        dirty = self.request("GET", self.tenant_path(f"/dirty-conversations?{query}"))
        items = dirty.get("items", [])
        if not items:
            print("dirty: none")
            return 0

        for item in items:
            conversation_id = item["conversation_id"]
            encoded = urllib.parse.quote(conversation_id, safe="")
            page = self.request("GET", self.tenant_path(f"/conversations/{encoded}/messages?limit=200"))
            messages = page.get("messages", [])
            print(f"conversation={conversation_id} max_seq={item['max_seq']} messages={len(messages)}")
            for message in messages:
                label = message.get("sender_display_name") or message.get("direction")
                text = message.get("text") or f"[{message.get('message_type')}]"
                print(f"  seq={message['conversation_seq']} {label}: {text}")
                media_id = message.get("media_id")
                if media_id:
                    media = self.request("GET", self.tenant_path(f"/media/{media_id}"))
                    download = self.request("GET", self.tenant_path(f"/media/{media_id}/download-url"))
                    print(
                        "    media "
                        f"id={media_id} bucket={media.get('bucket')} key={media.get('object_key')} "
                        f"url={download.get('url')}"
                    )
            self.request(
                "POST",
                self.tenant_path(f"/dirty-conversations/{encoded}/ack"),
                {
                    "consumer_id": self.consumer_id,
                    "processed_until_seq": item["max_seq"],
                    "lease_token": item["lease_token"],
                },
            )
            print(f"  acked through seq {item['max_seq']}")
        return len(items)


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulate a simple external RustZap backend.")
    parser.add_argument("--seed", action="store_true", help="create sample text, image, and group messages")
    parser.add_argument("--loop", action="store_true", help="keep polling")
    parser.add_argument("--interval", type=float, default=2.0, help="poll interval in seconds")
    args = parser.parse_args()

    client = RustZapClient()
    print(
        "rustzap "
        f"base={client.base_url} project={client.project_id} company={client.company_id} "
        f"consumer={client.consumer_id}"
    )
    print(
        "r2 "
        f"bucket={env('R2_DEV_BUCKET', env('R2_BUCKET', 'devbucket'))} "
        f"public_url={env('R2_DEV_PUBLIC_URL', env('R2_PUBLIC_URL', 'not-set'))}"
    )
    client.ensure_tenant()
    if args.seed:
        client.seed()

    while True:
        client.poll_once()
        if not args.loop:
            break
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
