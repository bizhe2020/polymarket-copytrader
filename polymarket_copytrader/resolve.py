from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote

from .api import PolymarketPublicApi

WALLET_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
PROFILE_URL_RE = re.compile(r"/profile/([^/?#]+)")
HANDLE_URL_RE = re.compile(r"/@([^/?#]+)")


def normalize_profile_input(value: str) -> str:
    stripped = value.strip()
    if WALLET_RE.match(stripped):
        return stripped
    match = PROFILE_URL_RE.search(stripped)
    if not match:
        match = HANDLE_URL_RE.search(stripped)
    if match:
        stripped = unquote(match.group(1))
    return stripped.lstrip("@").strip().lower()


def resolve_target_wallet(
    api: PolymarketPublicApi, profile_or_wallet: Optional[str], wallet_override: Optional[str]
) -> Tuple[str, Dict[str, object]]:
    if wallet_override:
        return wallet_override, {"source": "config.wallet"}

    if not profile_or_wallet:
        raise ValueError("target.profile 和 target.wallet 至少要提供一个。")

    normalized = normalize_profile_input(profile_or_wallet)
    if WALLET_RE.match(normalized):
        profile = api.get_public_profile(normalized)
        return normalized, {
            "source": "direct_wallet",
            "profile": {
                "name": profile.get("name"),
                "pseudonym": profile.get("pseudonym"),
                "proxyWallet": profile.get("proxyWallet"),
            },
        }

    candidates = _search_candidates(api, normalized)
    if not candidates:
        raise ValueError(
            f"无法通过 public-search 解析 `{profile_or_wallet}`，请在配置里手工设置 target.wallet。"
        )

    best = candidates[0]
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
        raise ValueError(
            "profile 解析结果不唯一，请手工设置 target.wallet，避免跟错账户。"
        )

    profile = best[1]
    wallet = str(profile["proxyWallet"])
    return wallet, {
        "source": "public-search",
        "score": best[0],
        "profile": {
            "name": profile.get("name"),
            "pseudonym": profile.get("pseudonym"),
            "proxyWallet": wallet,
        },
    }


def _search_candidates(
    api: PolymarketPublicApi, normalized: str
) -> List[Tuple[int, Dict[str, object]]]:
    results = []
    for query in [normalized, f"@{normalized}"]:
        for profile in api.public_search_profiles(query):
            wallet = str(profile.get("proxyWallet") or "")
            if not wallet:
                continue
            score = _score_profile(profile, normalized)
            if score > 0:
                results.append((score, profile))
    deduped: Dict[str, Tuple[int, Dict[str, object]]] = {}
    for score, profile in results:
        wallet = str(profile["proxyWallet"]).lower()
        previous = deduped.get(wallet)
        if previous is None or score > previous[0]:
            deduped[wallet] = (score, profile)
    return sorted(deduped.values(), key=lambda item: item[0], reverse=True)


def _score_profile(profile: Dict[str, object], normalized: str) -> int:
    fields = [
        str(profile.get("name") or "").strip().lower(),
        str(profile.get("pseudonym") or "").strip().lower(),
    ]
    score = 0
    for field in fields:
        if not field:
            continue
        if field == normalized:
            score = max(score, 100)
        elif field.replace(" ", "") == normalized.replace(" ", ""):
            score = max(score, 95)
        elif normalized in field:
            score = max(score, 60)
    return score
