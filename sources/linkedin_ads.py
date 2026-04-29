"""
LinkedIn Ads load source.

Reads LinkedIn Marketing data via the LinkedIn Marketing API v2 using
an OAuth2 access token (or client credentials for server-to-server apps).

Auth config keys under connection:
  access_token      OAuth2 access token (required)
  account_id        LinkedIn Ad Account ID (e.g. "123456789") — required
                    Find it in Campaign Manager URL: /campaignmanager/accounts/urn:li:sponsoredAccount:123456789

Tables:
  accounts              Ad account details
  campaign_groups       Campaign groups (top-level grouping)
  campaigns             Campaigns (under campaign groups)
  creatives             Ads/creatives
  ad_analytics          Daily performance by campaign (impressions, clicks, spend, conversions)
  ad_analytics_creative Daily performance by creative
  conversions           Conversion actions defined in the account
  audience_counts       Audience size estimates

Per-table config (under tables_config):
  date_range_days   How many days back for incremental loads (default: 30)
  start_date        Absolute start date for full snapshot, YYYY-MM-DD (optional)
  fields            List of analytics fields to include (default: all standard metrics)
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional

import requests

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

_BASE = "https://api.linkedin.com/v2"
_REST = "https://api.linkedin.com/rest"

_SCHEMAS: Dict[str, List[ColumnSchema]] = {
    "accounts": [
        ColumnSchema("id",            "bigint",  primary_key=True),
        ColumnSchema("name",          "varchar"),
        ColumnSchema("status",        "varchar"),
        ColumnSchema("type",          "varchar"),
        ColumnSchema("currency",      "varchar"),
        ColumnSchema("total_budget",  "double"),
        ColumnSchema("created_time",  "bigint"),
        ColumnSchema("last_modified", "bigint"),
    ],
    "campaign_groups": [
        ColumnSchema("id",              "bigint",  primary_key=True),
        ColumnSchema("name",            "varchar"),
        ColumnSchema("status",          "varchar"),
        ColumnSchema("account_id",      "bigint"),
        ColumnSchema("total_budget",    "double"),
        ColumnSchema("start_date",      "varchar"),
        ColumnSchema("end_date",        "varchar"),
        ColumnSchema("created_time",    "bigint"),
        ColumnSchema("last_modified",   "bigint"),
    ],
    "campaigns": [
        ColumnSchema("id",                   "bigint",  primary_key=True),
        ColumnSchema("name",                 "varchar"),
        ColumnSchema("status",               "varchar"),
        ColumnSchema("campaign_group_id",    "bigint"),
        ColumnSchema("account_id",           "bigint"),
        ColumnSchema("type",                 "varchar"),
        ColumnSchema("objective_type",       "varchar"),
        ColumnSchema("cost_type",            "varchar"),
        ColumnSchema("unit_cost_amount",     "double"),
        ColumnSchema("daily_budget_amount",  "double"),
        ColumnSchema("total_budget_amount",  "double"),
        ColumnSchema("created_time",         "bigint"),
        ColumnSchema("last_modified",        "bigint"),
    ],
    "creatives": [
        ColumnSchema("id",            "bigint",  primary_key=True),
        ColumnSchema("campaign_id",   "bigint"),
        ColumnSchema("status",        "varchar"),
        ColumnSchema("type",          "varchar"),
        ColumnSchema("reference",     "varchar"),
        ColumnSchema("created_time",  "bigint"),
        ColumnSchema("last_modified", "bigint"),
    ],
    "ad_analytics": [
        ColumnSchema("campaign_id",            "bigint",  primary_key=True),
        ColumnSchema("date",                   "varchar", primary_key=True),
        ColumnSchema("impressions",            "bigint"),
        ColumnSchema("clicks",                 "bigint"),
        ColumnSchema("cost_in_local_currency", "double"),
        ColumnSchema("cost_in_usd",            "double"),
        ColumnSchema("conversions",            "bigint"),
        ColumnSchema("external_website_conversions", "bigint"),
        ColumnSchema("leads",                  "bigint"),
        ColumnSchema("video_views",            "bigint"),
        ColumnSchema("viral_impressions",      "bigint"),
        ColumnSchema("viral_clicks",           "bigint"),
        ColumnSchema("ctr",                    "double"),
        ColumnSchema("average_cpc",            "double"),
        ColumnSchema("average_cpm",            "double"),
    ],
    "ad_analytics_creative": [
        ColumnSchema("creative_id",            "bigint",  primary_key=True),
        ColumnSchema("campaign_id",            "bigint"),
        ColumnSchema("date",                   "varchar", primary_key=True),
        ColumnSchema("impressions",            "bigint"),
        ColumnSchema("clicks",                 "bigint"),
        ColumnSchema("cost_in_local_currency", "double"),
        ColumnSchema("cost_in_usd",            "double"),
        ColumnSchema("conversions",            "bigint"),
        ColumnSchema("ctr",                    "double"),
        ColumnSchema("average_cpc",            "double"),
    ],
    "conversions": [
        ColumnSchema("id",              "bigint",  primary_key=True),
        ColumnSchema("name",            "varchar"),
        ColumnSchema("enabled",         "boolean"),
        ColumnSchema("type",            "varchar"),
        ColumnSchema("attribution_type","varchar"),
        ColumnSchema("value_in_local_currency", "double"),
        ColumnSchema("account_id",      "bigint"),
        ColumnSchema("created_time",    "bigint"),
        ColumnSchema("last_modified",   "bigint"),
    ],
    "audience_counts": [
        ColumnSchema("campaign_id",     "bigint",  primary_key=True),
        ColumnSchema("audience_count",  "bigint"),
        ColumnSchema("fetched_at",      "varchar"),
    ],
}

_ANALYTICS_FIELDS = [
    "impressions", "clicks", "costInLocalCurrency", "costInUsd",
    "externalWebsiteConversions", "externalWebsitePostClickConversions",
    "externalWebsitePostViewConversions", "leads", "viralImpressions",
    "viralClicks", "viralFollows", "videoViews", "videoFirstQuartileCompletions",
    "videoMidpointCompletions", "videoThirdQuartileCompletions",
    "videoCompletions", "actionClicks", "adUnitClicks", "commentLikes",
    "comments", "companyPageClicks", "follows", "fullScreenPlays",
    "likes", "opens", "otherEngagements", "sends", "shares",
    "textUrlClicks", "totalEngagements",
]


def _urn_id(urn: str) -> Optional[int]:
    """Extract numeric ID from a LinkedIn URN like urn:li:sponsoredCampaign:123."""
    try:
        return int(urn.split(":")[-1])
    except (ValueError, AttributeError, IndexError):
        return None


class LinkedInAdsSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._token      = conn.get("access_token", "")
        self._account_id = str(conn.get("account_id", "")).replace("urn:li:sponsoredAccount:", "")
        self._session    = None

    def connect(self):
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self._token}",
            "LinkedIn-Version": "202401",
            "X-Restli-Protocol-Version": "2.0.0",
        })
        # Verify token and account access
        r = self._session.get(
            f"{_REST}/adAccounts/{self._account_id}",
            timeout=15,
        )
        r.raise_for_status()
        logger.info("[linkedin_ads/%s] Connected to account %s", self.name, self._account_id)

    def close(self):
        if self._session:
            self._session.close()

    def _get(self, url: str, params: dict = None) -> dict:
        params = params or {}
        for attempt in range(3):
            r = self._session.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                logger.warning("[linkedin_ads] Rate limited — sleeping %ds", wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        raise RuntimeError("LinkedIn API rate limit exceeded after 3 retries")

    def _paginate(self, url: str, params: dict = None) -> List[dict]:
        params = {**(params or {}), "start": 0, "count": 100}
        results = []
        while True:
            data = self._get(url, params)
            elements = data.get("elements", [])
            results.extend(elements)
            paging = data.get("paging", {})
            total = paging.get("total", 0)
            if params["start"] + len(elements) >= total or not elements:
                break
            params["start"] += len(elements)
        return results

    def _date_range(self, table: str, start_after: Any = None) -> tuple[date, date]:
        table_cfg = self.cfg.get("tables_config", {}).get(table, {})
        end = date.today()

        if start_after:
            try:
                last = date.fromisoformat(str(start_after))
                start = last + timedelta(days=1)
            except ValueError:
                start = end - timedelta(days=30)
        elif table_cfg.get("start_date"):
            start = date.fromisoformat(table_cfg["start_date"])
        else:
            days_back = int(table_cfg.get("date_range_days", 30))
            start = end - timedelta(days=days_back)

        return start, end

    def _fetch_accounts(self) -> List[dict]:
        data = self._get(f"{_REST}/adAccounts/{self._account_id}")
        acc = data
        budget = acc.get("totalBudget", {})
        return [{
            "id":            _urn_id(acc.get("id", "")),
            "name":          acc.get("name"),
            "status":        acc.get("status"),
            "type":          acc.get("type"),
            "currency":      acc.get("currency"),
            "total_budget":  float(budget.get("amount", 0)) if budget else None,
            "created_time":  acc.get("changeAuditStamps", {}).get("created", {}).get("time"),
            "last_modified": acc.get("changeAuditStamps", {}).get("lastModified", {}).get("time"),
        }]

    def _fetch_campaign_groups(self) -> List[dict]:
        elements = self._paginate(
            f"{_REST}/adCampaignGroups",
            {"account": f"urn:li:sponsoredAccount:{self._account_id}"},
        )
        rows = []
        for e in elements:
            budget = e.get("totalBudget", {})
            sd = e.get("runSchedule", {}).get("start")
            ed = e.get("runSchedule", {}).get("end")
            rows.append({
                "id":            _urn_id(e.get("id", "")),
                "name":          e.get("name"),
                "status":        e.get("status"),
                "account_id":    int(self._account_id),
                "total_budget":  float(budget.get("amount", 0)) if budget else None,
                "start_date":    _ms_to_date(sd),
                "end_date":      _ms_to_date(ed),
                "created_time":  e.get("changeAuditStamps", {}).get("created", {}).get("time"),
                "last_modified": e.get("changeAuditStamps", {}).get("lastModified", {}).get("time"),
            })
        return rows

    def _fetch_campaigns(self) -> List[dict]:
        elements = self._paginate(
            f"{_REST}/adCampaigns",
            {"account": f"urn:li:sponsoredAccount:{self._account_id}"},
        )
        rows = []
        for e in elements:
            ub = e.get("unitCost", {})
            db = e.get("dailyBudget", {})
            tb = e.get("totalBudget", {})
            rows.append({
                "id":                  _urn_id(e.get("id", "")),
                "name":                e.get("name"),
                "status":              e.get("status"),
                "campaign_group_id":   _urn_id(e.get("campaignGroup", "")),
                "account_id":          int(self._account_id),
                "type":                e.get("type"),
                "objective_type":      e.get("objectiveType"),
                "cost_type":           e.get("costType"),
                "unit_cost_amount":    float(ub.get("amount", 0)) if ub else None,
                "daily_budget_amount": float(db.get("amount", 0)) if db else None,
                "total_budget_amount": float(tb.get("amount", 0)) if tb else None,
                "created_time":        e.get("changeAuditStamps", {}).get("created", {}).get("time"),
                "last_modified":       e.get("changeAuditStamps", {}).get("lastModified", {}).get("time"),
            })
        return rows

    def _fetch_creatives(self) -> List[dict]:
        elements = self._paginate(
            f"{_REST}/adCreatives",
            {"account": f"urn:li:sponsoredAccount:{self._account_id}"},
        )
        rows = []
        for e in elements:
            rows.append({
                "id":            _urn_id(e.get("id", "")),
                "campaign_id":   _urn_id(e.get("campaign", "")),
                "status":        e.get("status"),
                "type":          e.get("type"),
                "reference":     e.get("reference"),
                "created_time":  e.get("changeAuditStamps", {}).get("created", {}).get("time"),
                "last_modified": e.get("changeAuditStamps", {}).get("lastModified", {}).get("time"),
            })
        return rows

    def _fetch_analytics(self, pivot: str, start: date, end: date) -> List[dict]:
        """Fetch ad analytics grouped by campaign or creative, by day."""
        params = {
            "q":                     "analytics",
            "pivot":                 pivot,
            "timeGranularity":       "DAILY",
            "accounts":              f"urn:li:sponsoredAccount:{self._account_id}",
            "dateRange.start.year":  start.year,
            "dateRange.start.month": start.month,
            "dateRange.start.day":   start.day,
            "dateRange.end.year":    end.year,
            "dateRange.end.month":   end.month,
            "dateRange.end.day":     end.day,
            "fields":                ",".join(_ANALYTICS_FIELDS),
            "count":                 500,
        }
        data = self._get(f"{_BASE}/adAnalytics", params)
        return data.get("elements", [])

    def _parse_analytics(self, element: dict, pivot: str) -> dict:
        dr = element.get("dateRange", {})
        s = dr.get("start", {})
        date_str = f"{s.get('year', '')}-{s.get('month', 0):02d}-{s.get('day', 0):02d}"
        costs = element.get("costInLocalCurrency")
        cusd  = element.get("costInUsd")
        imp   = element.get("impressions", 0) or 0
        clk   = element.get("clicks", 0) or 0

        row: Dict[str, Any] = {
            "date":                        date_str,
            "impressions":                 imp,
            "clicks":                      clk,
            "cost_in_local_currency":      float(costs) if costs else None,
            "cost_in_usd":                 float(cusd) if cusd else None,
            "conversions":                 element.get("externalWebsiteConversions"),
            "external_website_conversions":element.get("externalWebsiteConversions"),
            "leads":                       element.get("leads"),
            "video_views":                 element.get("videoViews"),
            "viral_impressions":           element.get("viralImpressions"),
            "viral_clicks":                element.get("viralClicks"),
            "ctr":                         (clk / imp) if imp else 0.0,
            "average_cpc":                 (float(costs) / clk) if costs and clk else None,
            "average_cpm":                 (float(costs) / imp * 1000) if costs and imp else None,
        }

        if pivot == "CAMPAIGN":
            row["campaign_id"] = _urn_id(element.get("pivotValue", ""))
        else:
            row["creative_id"] = _urn_id(element.get("pivotValue", ""))
            row["campaign_id"] = None

        return row

    def _fetch_conversions(self) -> List[dict]:
        elements = self._paginate(
            f"{_REST}/conversions",
            {"account": f"urn:li:sponsoredAccount:{self._account_id}"},
        )
        rows = []
        for e in elements:
            val = e.get("valueInLocalCurrency", {})
            rows.append({
                "id":               _urn_id(e.get("id", "")),
                "name":             e.get("name"),
                "enabled":          e.get("enabled"),
                "type":             e.get("type"),
                "attribution_type": e.get("attributionType"),
                "value_in_local_currency": float(val.get("amount", 0)) if val else None,
                "account_id":       int(self._account_id),
                "created_time":     e.get("changeAuditStamps", {}).get("created", {}).get("time"),
                "last_modified":    e.get("changeAuditStamps", {}).get("lastModified", {}).get("time"),
            })
        return rows

    def _fetch_audience_counts(self) -> List[dict]:
        campaigns = self._fetch_campaigns()
        rows = []
        for c in campaigns:
            cid = c["id"]
            try:
                data = self._get(
                    f"{_REST}/adCampaignAudienceCounts",
                    {"campaign": f"urn:li:sponsoredCampaign:{cid}"},
                )
                count = data.get("elements", [{}])[0].get("audienceCount")
                rows.append({
                    "campaign_id":    cid,
                    "audience_count": count,
                    "fetched_at":     date.today().isoformat(),
                })
            except Exception as e:
                logger.debug("[linkedin_ads] audience count error for campaign %s: %s", cid, e)
        return rows

    def _dispatch(self, table: str, start: date = None, end: date = None) -> List[dict]:
        if table == "accounts":
            return self._fetch_accounts()
        if table == "campaign_groups":
            return self._fetch_campaign_groups()
        if table == "campaigns":
            return self._fetch_campaigns()
        if table == "creatives":
            return self._fetch_creatives()
        if table == "ad_analytics":
            elems = self._fetch_analytics("CAMPAIGN", start, end)
            return [self._parse_analytics(e, "CAMPAIGN") for e in elems]
        if table == "ad_analytics_creative":
            elems = self._fetch_analytics("CREATIVE", start, end)
            return [self._parse_analytics(e, "CREATIVE") for e in elems]
        if table == "conversions":
            return self._fetch_conversions()
        if table == "audience_counts":
            return self._fetch_audience_counts()
        raise ValueError(f"Unknown LinkedIn Ads table: {table}")

    def get_schema(self, table: str) -> List[ColumnSchema]:
        return _SCHEMAS.get(table, [])

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        start, end = self._date_range(table)
        rows = self._dispatch(table, start, end)
        logger.info("[linkedin_ads/%s/%s] Snapshot — %d rows", self.name, table, len(rows))
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("date") or row.get("last_modified"),
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        start, end = self._date_range(table, start_after)

        if table in ("ad_analytics", "ad_analytics_creative"):
            if start > end:
                logger.info("[linkedin_ads/%s/%s] Already up to date", self.name, table)
                return
        rows = self._dispatch(table, start, end)
        logger.info("[linkedin_ads/%s/%s] Incremental — %d rows (since %s)",
                    self.name, table, len(rows), start_after)

        count = 0
        for row in rows:
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("date") or row.get("last_modified"),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        if table in ("ad_analytics", "ad_analytics_creative"):
            return "date"
        return "last_modified"


def _ms_to_date(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()
    except Exception:
        return None
