from typing import Mapping

TOUCHABLE_CAMPAIGN_STATUSES = {"ENABLED", "PAUSED"}
REMOVED_CAMPAIGN_STATUS = "REMOVED"
DEFAULT_INACTIVE_PREFIXES = ("zzz.", "zzz_")


def get_campaign_status(campaign: Mapping[str, object]) -> str:
    return str(campaign.get("campaignStatus") or campaign.get("status") or "").strip().upper()


def is_inactive_campaign_name(
    name: object,
    *,
    inactive_prefixes: tuple[str, ...] = DEFAULT_INACTIVE_PREFIXES,
) -> bool:
    cleaned = str(name or "").strip().lower()
    if not cleaned:
        return False
    return any(cleaned.startswith(prefix) for prefix in inactive_prefixes)


def should_include_campaign_in_row(campaign: Mapping[str, object]) -> bool:
    return get_campaign_status(campaign) != REMOVED_CAMPAIGN_STATUS


def has_any_active_campaign(
    campaigns: object,
    *,
    inactive_prefixes: tuple[str, ...] = DEFAULT_INACTIVE_PREFIXES,
) -> bool:
    """
    Active campaign rule:
    - status must be ENABLED or PAUSED
    - campaign name must not be inactive (zzz.*)
    - at least one remaining campaign must be ENABLED
    """
    if not isinstance(campaigns, list) or not campaigns:
        return False

    for campaign in campaigns:
        if not isinstance(campaign, Mapping):
            continue
        campaign_status = get_campaign_status(campaign)
        if campaign_status not in TOUCHABLE_CAMPAIGN_STATUSES:
            continue

        campaign_name = campaign.get("campaignName")
        if is_inactive_campaign_name(
            campaign_name,
            inactive_prefixes=inactive_prefixes,
        ):
            continue

        if campaign_status == "ENABLED":
            return True

    return False


def should_filter_row(
    *,
    no_allocation: bool,
    no_active_campaigns: bool,
    no_spent: bool,
) -> bool:
    return no_allocation and no_active_campaigns and no_spent
