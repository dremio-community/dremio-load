"""
Google Ads load source.

Reads Google Ads data via the Google Ads API (GAQL) using the google-ads
Python client library.

Auth config keys under connection:
  developer_token     Google Ads developer token (required)
  client_id           OAuth2 client ID
  client_secret       OAuth2 client secret
  refresh_token       OAuth2 refresh token
  customer_id         Google Ads customer ID (without dashes, e.g. "1234567890")
  login_customer_id   MCC/manager account ID (optional — needed when accessing
                      sub-accounts via a manager account)

Tables (what to load):
  campaigns               Campaign metadata
  ad_groups               Ad group metadata
  ads                     Ad/creative metadata
  keywords                Keyword metadata
  campaign_performance    Daily perf by campaign (impressions, clicks, cost, conversions)
  ad_group_performance    Daily perf by ad group
  search_terms            Search term report
  geographic_performance  Geo performance report
  audience_performance    Audience performance report

Per-table config (under tables_config):
  date_range_days   How many days back for incremental loads (default: 30)
  start_date        Absolute start date for full snapshot, YYYY-MM-DD (optional)
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Generator, List, Optional

from core.event import ChangeEvent, ColumnSchema, Operation
from sources.base import LoadSource

logger = logging.getLogger(__name__)

# ── GAQL query templates per table ────────────────────────────────────────────

_QUERIES: Dict[str, str] = {
    "campaigns": """
        SELECT
            campaign.id, campaign.name, campaign.status,
            campaign.advertising_channel_type, campaign.bidding_strategy_type,
            campaign.start_date, campaign.end_date,
            campaign_budget.amount_micros, campaign.optimization_score
        FROM campaign
        ORDER BY campaign.id
    """,
    "ad_groups": """
        SELECT
            ad_group.id, ad_group.name, ad_group.status,
            ad_group.type, campaign.id, campaign.name,
            ad_group.cpc_bid_micros, ad_group.target_cpa_micros
        FROM ad_group
        ORDER BY ad_group.id
    """,
    "ads": """
        SELECT
            ad_group_ad.ad.id, ad_group_ad.ad.name,
            ad_group_ad.ad.type, ad_group_ad.status,
            ad_group.id, ad_group.name,
            campaign.id, campaign.name,
            ad_group_ad.ad.final_urls,
            ad_group_ad.ad.display_url
        FROM ad_group_ad
        ORDER BY ad_group_ad.ad.id
    """,
    "keywords": """
        SELECT
            ad_group_criterion.criterion_id,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.status,
            ad_group_criterion.cpc_bid_micros,
            ad_group_criterion.quality_info.quality_score,
            ad_group.id, ad_group.name,
            campaign.id, campaign.name
        FROM ad_group_criterion
        WHERE ad_group_criterion.type = 'KEYWORD'
        ORDER BY ad_group_criterion.criterion_id
    """,
    "campaign_performance": """
        SELECT
            campaign.id, campaign.name, campaign.status,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value,
            metrics.ctr, metrics.average_cpc,
            metrics.video_views, metrics.view_through_conversions
        FROM campaign
        WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'
        ORDER BY segments.date, campaign.id
    """,
    "ad_group_performance": """
        SELECT
            ad_group.id, ad_group.name,
            campaign.id, campaign.name,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.conversions_value,
            metrics.ctr, metrics.average_cpc
        FROM ad_group
        WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'
        ORDER BY segments.date, ad_group.id
    """,
    "search_terms": """
        SELECT
            search_term_view.search_term,
            search_term_view.status,
            campaign.id, campaign.name,
            ad_group.id, ad_group.name,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.ctr, metrics.average_cpc
        FROM search_term_view
        WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'
        ORDER BY segments.date
    """,
    "geographic_performance": """
        SELECT
            geographic_view.country_criterion_id,
            geographic_view.resource_name,
            campaign.id, campaign.name,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions, metrics.ctr
        FROM geographic_view
        WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'
        ORDER BY segments.date
    """,
    "audience_performance": """
        SELECT
            user_list.id, user_list.name,
            ad_group.id, ad_group.name,
            campaign.id, campaign.name,
            segments.date,
            metrics.impressions, metrics.clicks, metrics.cost_micros,
            metrics.conversions
        FROM ad_group_audience_view
        WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'
        ORDER BY segments.date
    """,
}

# Schema definitions per table
_SCHEMAS: Dict[str, List[ColumnSchema]] = {
    "campaigns": [
        ColumnSchema("campaign_id",                "bigint",  primary_key=True),
        ColumnSchema("campaign_name",              "varchar"),
        ColumnSchema("status",                     "varchar"),
        ColumnSchema("advertising_channel_type",   "varchar"),
        ColumnSchema("bidding_strategy_type",      "varchar"),
        ColumnSchema("start_date",                 "varchar"),
        ColumnSchema("end_date",                   "varchar"),
        ColumnSchema("budget_amount_micros",       "bigint"),
        ColumnSchema("optimization_score",         "double"),
    ],
    "ad_groups": [
        ColumnSchema("ad_group_id",        "bigint",  primary_key=True),
        ColumnSchema("ad_group_name",      "varchar"),
        ColumnSchema("status",             "varchar"),
        ColumnSchema("type",               "varchar"),
        ColumnSchema("campaign_id",        "bigint"),
        ColumnSchema("campaign_name",      "varchar"),
        ColumnSchema("cpc_bid_micros",     "bigint"),
        ColumnSchema("target_cpa_micros",  "bigint"),
    ],
    "ads": [
        ColumnSchema("ad_id",          "bigint",  primary_key=True),
        ColumnSchema("ad_name",        "varchar"),
        ColumnSchema("ad_type",        "varchar"),
        ColumnSchema("status",         "varchar"),
        ColumnSchema("ad_group_id",    "bigint"),
        ColumnSchema("ad_group_name",  "varchar"),
        ColumnSchema("campaign_id",    "bigint"),
        ColumnSchema("campaign_name",  "varchar"),
        ColumnSchema("final_urls",     "varchar"),
        ColumnSchema("display_url",    "varchar"),
    ],
    "keywords": [
        ColumnSchema("criterion_id",    "bigint",  primary_key=True),
        ColumnSchema("keyword_text",    "varchar"),
        ColumnSchema("match_type",      "varchar"),
        ColumnSchema("status",          "varchar"),
        ColumnSchema("cpc_bid_micros",  "bigint"),
        ColumnSchema("quality_score",   "bigint"),
        ColumnSchema("ad_group_id",     "bigint"),
        ColumnSchema("ad_group_name",   "varchar"),
        ColumnSchema("campaign_id",     "bigint"),
        ColumnSchema("campaign_name",   "varchar"),
    ],
    "campaign_performance": [
        ColumnSchema("campaign_id",             "bigint",  primary_key=True),
        ColumnSchema("campaign_name",           "varchar"),
        ColumnSchema("status",                  "varchar"),
        ColumnSchema("date",                    "varchar", primary_key=True),
        ColumnSchema("impressions",             "bigint"),
        ColumnSchema("clicks",                  "bigint"),
        ColumnSchema("cost_micros",             "bigint"),
        ColumnSchema("conversions",             "double"),
        ColumnSchema("conversions_value",       "double"),
        ColumnSchema("ctr",                     "double"),
        ColumnSchema("average_cpc",             "double"),
        ColumnSchema("video_views",             "bigint"),
        ColumnSchema("view_through_conversions","bigint"),
    ],
    "ad_group_performance": [
        ColumnSchema("ad_group_id",       "bigint",  primary_key=True),
        ColumnSchema("ad_group_name",     "varchar"),
        ColumnSchema("campaign_id",       "bigint"),
        ColumnSchema("campaign_name",     "varchar"),
        ColumnSchema("date",              "varchar", primary_key=True),
        ColumnSchema("impressions",       "bigint"),
        ColumnSchema("clicks",            "bigint"),
        ColumnSchema("cost_micros",       "bigint"),
        ColumnSchema("conversions",       "double"),
        ColumnSchema("conversions_value", "double"),
        ColumnSchema("ctr",               "double"),
        ColumnSchema("average_cpc",       "double"),
    ],
    "search_terms": [
        ColumnSchema("search_term",   "varchar"),
        ColumnSchema("status",        "varchar"),
        ColumnSchema("campaign_id",   "bigint"),
        ColumnSchema("campaign_name", "varchar"),
        ColumnSchema("ad_group_id",   "bigint"),
        ColumnSchema("ad_group_name", "varchar"),
        ColumnSchema("date",          "varchar"),
        ColumnSchema("impressions",   "bigint"),
        ColumnSchema("clicks",        "bigint"),
        ColumnSchema("cost_micros",   "bigint"),
        ColumnSchema("conversions",   "double"),
        ColumnSchema("ctr",           "double"),
        ColumnSchema("average_cpc",   "double"),
    ],
    "geographic_performance": [
        ColumnSchema("country_criterion_id", "bigint"),
        ColumnSchema("resource_name",        "varchar"),
        ColumnSchema("campaign_id",          "bigint"),
        ColumnSchema("campaign_name",        "varchar"),
        ColumnSchema("date",                 "varchar"),
        ColumnSchema("impressions",          "bigint"),
        ColumnSchema("clicks",               "bigint"),
        ColumnSchema("cost_micros",          "bigint"),
        ColumnSchema("conversions",          "double"),
        ColumnSchema("ctr",                  "double"),
    ],
    "audience_performance": [
        ColumnSchema("user_list_id",   "bigint"),
        ColumnSchema("user_list_name", "varchar"),
        ColumnSchema("ad_group_id",    "bigint"),
        ColumnSchema("ad_group_name",  "varchar"),
        ColumnSchema("campaign_id",    "bigint"),
        ColumnSchema("campaign_name",  "varchar"),
        ColumnSchema("date",           "varchar"),
        ColumnSchema("impressions",    "bigint"),
        ColumnSchema("clicks",         "bigint"),
        ColumnSchema("cost_micros",    "bigint"),
        ColumnSchema("conversions",    "double"),
    ],
}

_PERF_TABLES = {
    "campaign_performance", "ad_group_performance", "search_terms",
    "geographic_performance", "audience_performance",
}


def _flatten_row(row) -> Dict[str, Any]:
    """Flatten a Google Ads API row object into a plain dict."""
    result = {}

    def _walk(obj, prefix=""):
        if hasattr(obj, "__class__") and hasattr(obj, "__dict__"):
            for field in obj.__class__._meta.fields:  # protobuf fields
                try:
                    val = getattr(obj, field.name)
                    key = f"{prefix}{field.name}" if not prefix else f"{prefix}_{field.name}"
                    if hasattr(val, "__class__") and hasattr(val, "__dict__") and not isinstance(val, (str, int, float, bool, type(None))):
                        _walk(val, key)
                    else:
                        result[key] = val
                except Exception:
                    pass
        return result

    # Simpler approach: use the row's __str__ / to_json if available
    try:
        import json
        from google.protobuf import json_format
        d = json.loads(json_format.MessageToJson(row._pb))
        # Flatten nested dict with underscore join
        def flatten(obj, prefix=""):
            flat = {}
            for k, v in obj.items():
                full_key = f"{prefix}_{k}" if prefix else k
                if isinstance(v, dict):
                    flat.update(flatten(v, full_key))
                elif isinstance(v, list):
                    flat[full_key] = str(v)
                else:
                    flat[full_key] = v
            return flat
        return flatten(d)
    except Exception:
        pass

    return result


def _map_row(table: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map a flattened API response row to our schema column names."""
    schema = _SCHEMAS.get(table, [])
    col_names = {col.name for col in schema}

    mappings = {
        "campaigns": {
            "campaign_id":              raw.get("campaign_id") or raw.get("id"),
            "campaign_name":            raw.get("campaign_name") or raw.get("name"),
            "status":                   raw.get("campaign_status") or raw.get("status"),
            "advertising_channel_type": raw.get("campaign_advertisingChannelType"),
            "bidding_strategy_type":    raw.get("campaign_biddingStrategyType"),
            "start_date":               raw.get("campaign_startDate"),
            "end_date":                 raw.get("campaign_endDate"),
            "budget_amount_micros":     raw.get("campaignBudget_amountMicros"),
            "optimization_score":       raw.get("campaign_optimizationScore"),
        },
        "ad_groups": {
            "ad_group_id":       raw.get("adGroup_id"),
            "ad_group_name":     raw.get("adGroup_name"),
            "status":            raw.get("adGroup_status"),
            "type":              raw.get("adGroup_type"),
            "campaign_id":       raw.get("campaign_id"),
            "campaign_name":     raw.get("campaign_name"),
            "cpc_bid_micros":    raw.get("adGroup_cpcBidMicros"),
            "target_cpa_micros": raw.get("adGroup_targetCpaMicros"),
        },
        "ads": {
            "ad_id":          raw.get("adGroupAd_ad_id"),
            "ad_name":        raw.get("adGroupAd_ad_name"),
            "ad_type":        raw.get("adGroupAd_ad_type"),
            "status":         raw.get("adGroupAd_status"),
            "ad_group_id":    raw.get("adGroup_id"),
            "ad_group_name":  raw.get("adGroup_name"),
            "campaign_id":    raw.get("campaign_id"),
            "campaign_name":  raw.get("campaign_name"),
            "final_urls":     str(raw.get("adGroupAd_ad_finalUrls", "")),
            "display_url":    raw.get("adGroupAd_ad_displayUrl"),
        },
        "keywords": {
            "criterion_id":   raw.get("adGroupCriterion_criterionId"),
            "keyword_text":   raw.get("adGroupCriterion_keyword_text"),
            "match_type":     raw.get("adGroupCriterion_keyword_matchType"),
            "status":         raw.get("adGroupCriterion_status"),
            "cpc_bid_micros": raw.get("adGroupCriterion_cpcBidMicros"),
            "quality_score":  raw.get("adGroupCriterion_qualityInfo_qualityScore"),
            "ad_group_id":    raw.get("adGroup_id"),
            "ad_group_name":  raw.get("adGroup_name"),
            "campaign_id":    raw.get("campaign_id"),
            "campaign_name":  raw.get("campaign_name"),
        },
        "campaign_performance": {
            "campaign_id":              raw.get("campaign_id"),
            "campaign_name":            raw.get("campaign_name"),
            "status":                   raw.get("campaign_status"),
            "date":                     raw.get("segments_date"),
            "impressions":              raw.get("metrics_impressions"),
            "clicks":                   raw.get("metrics_clicks"),
            "cost_micros":              raw.get("metrics_costMicros"),
            "conversions":              raw.get("metrics_conversions"),
            "conversions_value":        raw.get("metrics_conversionsValue"),
            "ctr":                      raw.get("metrics_ctr"),
            "average_cpc":              raw.get("metrics_averageCpc"),
            "video_views":              raw.get("metrics_videoViews"),
            "view_through_conversions": raw.get("metrics_viewThroughConversions"),
        },
        "ad_group_performance": {
            "ad_group_id":       raw.get("adGroup_id"),
            "ad_group_name":     raw.get("adGroup_name"),
            "campaign_id":       raw.get("campaign_id"),
            "campaign_name":     raw.get("campaign_name"),
            "date":              raw.get("segments_date"),
            "impressions":       raw.get("metrics_impressions"),
            "clicks":            raw.get("metrics_clicks"),
            "cost_micros":       raw.get("metrics_costMicros"),
            "conversions":       raw.get("metrics_conversions"),
            "conversions_value": raw.get("metrics_conversionsValue"),
            "ctr":               raw.get("metrics_ctr"),
            "average_cpc":       raw.get("metrics_averageCpc"),
        },
        "search_terms": {
            "search_term":  raw.get("searchTermView_searchTerm"),
            "status":       raw.get("searchTermView_status"),
            "campaign_id":  raw.get("campaign_id"),
            "campaign_name":raw.get("campaign_name"),
            "ad_group_id":  raw.get("adGroup_id"),
            "ad_group_name":raw.get("adGroup_name"),
            "date":         raw.get("segments_date"),
            "impressions":  raw.get("metrics_impressions"),
            "clicks":       raw.get("metrics_clicks"),
            "cost_micros":  raw.get("metrics_costMicros"),
            "conversions":  raw.get("metrics_conversions"),
            "ctr":          raw.get("metrics_ctr"),
            "average_cpc":  raw.get("metrics_averageCpc"),
        },
        "geographic_performance": {
            "country_criterion_id": raw.get("geographicView_countryCriterionId"),
            "resource_name":        raw.get("geographicView_resourceName"),
            "campaign_id":          raw.get("campaign_id"),
            "campaign_name":        raw.get("campaign_name"),
            "date":                 raw.get("segments_date"),
            "impressions":          raw.get("metrics_impressions"),
            "clicks":               raw.get("metrics_clicks"),
            "cost_micros":          raw.get("metrics_costMicros"),
            "conversions":          raw.get("metrics_conversions"),
            "ctr":                  raw.get("metrics_ctr"),
        },
        "audience_performance": {
            "user_list_id":   raw.get("userList_id"),
            "user_list_name": raw.get("userList_name"),
            "ad_group_id":    raw.get("adGroup_id"),
            "ad_group_name":  raw.get("adGroup_name"),
            "campaign_id":    raw.get("campaign_id"),
            "campaign_name":  raw.get("campaign_name"),
            "date":           raw.get("segments_date"),
            "impressions":    raw.get("metrics_impressions"),
            "clicks":         raw.get("metrics_clicks"),
            "cost_micros":    raw.get("metrics_costMicros"),
            "conversions":    raw.get("metrics_conversions"),
        },
    }
    return mappings.get(table, raw)


class GoogleAdsSource(LoadSource):

    def __init__(self, name: str, cfg: Dict[str, Any]):
        super().__init__(name, cfg)
        conn = cfg.get("connection", {})
        self._developer_token  = conn.get("developer_token", "")
        self._client_id        = conn.get("client_id", "")
        self._client_secret    = conn.get("client_secret", "")
        self._refresh_token    = conn.get("refresh_token", "")
        self._customer_id      = str(conn.get("customer_id", "")).replace("-", "")
        self._login_customer_id = str(conn.get("login_customer_id", "")).replace("-", "") or None
        self._client           = None

    def connect(self):
        from google.ads.googleads.client import GoogleAdsClient
        credentials = {
            "developer_token":   self._developer_token,
            "client_id":         self._client_id,
            "client_secret":     self._client_secret,
            "refresh_token":     self._refresh_token,
            "use_proto_plus":    True,
        }
        if self._login_customer_id:
            credentials["login_customer_id"] = self._login_customer_id

        self._client = GoogleAdsClient.load_from_dict(credentials)
        # Verify connectivity
        ga_service = self._client.get_service("GoogleAdsService")
        response = ga_service.search(
            customer_id=self._customer_id,
            query="SELECT customer.id FROM customer LIMIT 1",
        )
        list(response)
        logger.info("[google_ads/%s] Connected to customer %s", self.name, self._customer_id)

    def get_schema(self, table: str) -> List[ColumnSchema]:
        return _SCHEMAS.get(table, [])

    def _date_range(self, table: str, start_after: Any = None) -> tuple[str, str]:
        table_cfg = self.cfg.get("tables_config", {}).get(table, {})
        end_date = date.today().isoformat()

        if start_after:
            # Incremental: one day after the last loaded date
            try:
                last = date.fromisoformat(str(start_after))
                start_date = (last + timedelta(days=1)).isoformat()
            except ValueError:
                start_date = (date.today() - timedelta(days=30)).isoformat()
        elif table_cfg.get("start_date"):
            start_date = table_cfg["start_date"]
        else:
            days_back = int(table_cfg.get("date_range_days", 30))
            start_date = (date.today() - timedelta(days=days_back)).isoformat()

        return start_date, end_date

    def _run_query(self, query: str) -> List[Dict[str, Any]]:
        from google.protobuf import json_format
        import json

        ga_service = self._client.get_service("GoogleAdsService")
        response = ga_service.search(
            customer_id=self._customer_id,
            query=query.strip(),
        )
        rows = []
        for row in response:
            try:
                d = json.loads(json_format.MessageToJson(row._pb))
                flat = {}
                def _flatten(obj, prefix=""):
                    for k, v in obj.items():
                        key = f"{prefix}_{k}" if prefix else k
                        if isinstance(v, dict):
                            _flatten(v, key)
                        elif isinstance(v, list):
                            flat[key] = str(v)
                        else:
                            flat[key] = v
                _flatten(d)
                rows.append(flat)
            except Exception as e:
                logger.debug("[google_ads] Row parse error: %s", e)
        return rows

    def snapshot(self, table: str) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)
        if table in _PERF_TABLES:
            start_date, end_date = self._date_range(table)
            query = _QUERIES[table].format(start_date=start_date, end_date=end_date)
        else:
            query = _QUERIES[table]

        rows = self._run_query(query)
        logger.info("[google_ads/%s/%s] Snapshot — %d rows", self.name, table, len(rows))

        for raw in rows:
            row = _map_row(table, raw)
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("date") or row.get("campaign_id"),
            )

    def incremental_snapshot(
        self, table: str, cursor_col: str, start_after: Any, chunk_size: int
    ) -> Generator[ChangeEvent, None, None]:
        schema = self.get_schema(table)

        if table in _PERF_TABLES:
            start_date, end_date = self._date_range(table, start_after)
            if start_date > end_date:
                logger.info("[google_ads/%s/%s] Already up to date", self.name, table)
                return
            query = _QUERIES[table].format(start_date=start_date, end_date=end_date)
        else:
            # Entity tables: always full refresh (no delta API)
            query = _QUERIES[table]

        rows = self._run_query(query)
        logger.info("[google_ads/%s/%s] Incremental — %d rows (since %s)",
                    self.name, table, len(rows), start_after)

        count = 0
        for raw in rows:
            row = _map_row(table, raw)
            yield ChangeEvent(
                op=Operation.SNAPSHOT,
                source_name=self.name, source_table=table,
                before=None, after=row, schema=schema,
                timestamp=datetime.now(timezone.utc),
                offset=row.get("date") or row.get("campaign_id"),
            )
            count += 1
            if count >= chunk_size:
                return

    def get_cursor_column(self, table: str) -> str:
        return "date" if table in _PERF_TABLES else "campaign_id"

    def close(self):
        self._client = None
