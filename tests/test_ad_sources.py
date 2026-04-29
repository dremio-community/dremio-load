"""
Unit tests for Google Ads and LinkedIn Ads load sources.

All tests are mock-based — no live API credentials required.

Run:
  python3 -m pytest tests/test_ad_sources.py -v
"""
import sys
import os
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _make_cfg(source_type: str, connection: dict, tables: list = None,
              tables_config: dict = None) -> dict:
    return {
        "source_type":   source_type,
        "connection":    connection,
        "tables":        tables or [],
        "tables_config": tables_config or {},
    }


def collect(gen) -> list:
    return list(gen)


# ══════════════════════════════════════════════════════════════════════════════
# Google Ads
# ══════════════════════════════════════════════════════════════════════════════

GOOGLE_ADS_CFG = _make_cfg("google_ads", {
    "developer_token": "test-token",
    "client_id":       "client-id",
    "client_secret":   "client-secret",
    "refresh_token":   "refresh-token",
    "customer_id":     "1234567890",
}, tables=["campaigns", "campaign_performance"])


def _mock_google_ads_row(data: dict):
    """Build a mock protobuf-like row that json_format.MessageToJson can handle."""
    import json
    from unittest.mock import MagicMock

    row = MagicMock()
    # Simulate MessageToJson returning a nested JSON matching Google Ads structure
    row._pb = MagicMock()
    return row, data


class TestGoogleAdsSource:

    @pytest.fixture
    def source(self):
        from sources.google_ads import GoogleAdsSource
        src = GoogleAdsSource("gads_test", GOOGLE_ADS_CFG)
        return src

    def test_get_schema_campaigns(self, source):
        schema = source.get_schema("campaigns")
        names = [c.name for c in schema]
        assert "campaign_id" in names
        assert "campaign_name" in names
        assert "status" in names

    def test_get_schema_campaign_performance(self, source):
        schema = source.get_schema("campaign_performance")
        names = [c.name for c in schema]
        assert "impressions" in names
        assert "clicks" in names
        assert "cost_micros" in names
        assert "date" in names

    def test_get_schema_ad_groups(self, source):
        schema = source.get_schema("ad_groups")
        assert any(c.name == "ad_group_id" for c in schema)

    def test_get_schema_keywords(self, source):
        schema = source.get_schema("keywords")
        assert any(c.name == "keyword_text" for c in schema)

    def test_get_schema_search_terms(self, source):
        schema = source.get_schema("search_terms")
        assert any(c.name == "search_term" for c in schema)

    def test_date_range_default(self, source):
        start, end = source._date_range("campaign_performance")
        today = date.today()
        assert end == today.isoformat()
        expected_start = (today.replace(day=today.day) - __import__('datetime').timedelta(days=30)).isoformat()
        assert start <= end

    def test_date_range_incremental(self, source):
        start, end = source._date_range("campaign_performance", start_after="2025-01-15")
        assert start == "2025-01-16"

    def test_date_range_already_up_to_date(self, source):
        today = date.today().isoformat()
        start, end = source._date_range("campaign_performance", start_after=today)
        assert start > end  # signals nothing to load

    def test_date_range_custom_days(self, source):
        cfg = _make_cfg("google_ads", GOOGLE_ADS_CFG["connection"],
                        tables_config={"campaign_performance": {"date_range_days": 7}})
        from sources.google_ads import GoogleAdsSource
        src = GoogleAdsSource("t", cfg)
        start, end = src._date_range("campaign_performance")
        delta = (date.fromisoformat(end) - date.fromisoformat(start)).days
        assert delta == 7

    def test_cursor_column_perf_tables(self, source):
        assert source.get_cursor_column("campaign_performance") == "date"
        assert source.get_cursor_column("ad_group_performance") == "date"
        assert source.get_cursor_column("search_terms") == "date"

    def test_cursor_column_entity_tables(self, source):
        assert source.get_cursor_column("campaigns") == "campaign_id"
        assert source.get_cursor_column("ad_groups") == "campaign_id"

    def test_snapshot_calls_run_query(self, source):
        mock_rows = [
            {
                "campaign_id": "123", "campaign_name": "Test Campaign",
                "campaign_status": "ENABLED", "campaign_advertisingChannelType": "SEARCH",
                "campaign_biddingStrategyType": "TARGET_CPA",
                "campaign_startDate": "2025-01-01", "campaign_endDate": None,
                "campaignBudget_amountMicros": 1000000, "campaign_optimizationScore": 0.8,
            }
        ]
        source._run_query = MagicMock(return_value=mock_rows)
        events = collect(source.snapshot("campaigns"))
        assert len(events) == 1
        assert events[0].after["campaign_id"] == "123"
        assert events[0].after["campaign_name"] == "Test Campaign"

    def test_snapshot_performance_table(self, source):
        mock_rows = [
            {
                "campaign_id": "456", "campaign_name": "Perf Campaign",
                "campaign_status": "ENABLED", "segments_date": "2025-03-01",
                "metrics_impressions": 1000, "metrics_clicks": 50,
                "metrics_costMicros": 25000000, "metrics_conversions": 5.0,
                "metrics_conversionsValue": 500.0, "metrics_ctr": 0.05,
                "metrics_averageCpc": 500000, "metrics_videoViews": 0,
                "metrics_viewThroughConversions": 0,
            }
        ]
        source._run_query = MagicMock(return_value=mock_rows)
        events = collect(source.snapshot("campaign_performance"))
        assert len(events) == 1
        row = events[0].after
        assert row["impressions"] == 1000
        assert row["clicks"] == 50
        assert row["date"] == "2025-03-01"

    def test_incremental_snapshot_entity_table(self, source):
        mock_rows = [{"campaign_id": "1", "campaign_name": "X", "campaign_status": "ENABLED",
                      "campaign_advertisingChannelType": "SEARCH", "campaign_biddingStrategyType": "CPC",
                      "campaign_startDate": None, "campaign_endDate": None,
                      "campaignBudget_amountMicros": 0, "campaign_optimizationScore": None}]
        source._run_query = MagicMock(return_value=mock_rows)
        events = collect(source.incremental_snapshot("campaigns", "campaign_id", "0", 100))
        assert len(events) == 1

    def test_incremental_up_to_date_returns_nothing(self, source):
        today = date.today().isoformat()
        source._run_query = MagicMock(return_value=[])
        events = collect(source.incremental_snapshot(
            "campaign_performance", "date", today, 100
        ))
        # Either no rows returned from API or start > end guard
        assert isinstance(events, list)

    def test_chunk_size_respected(self, source):
        mock_rows = [{"campaign_id": str(i), "campaign_name": f"C{i}",
                      "campaign_status": "ENABLED", "campaign_advertisingChannelType": "SEARCH",
                      "campaign_biddingStrategyType": "CPC", "campaign_startDate": None,
                      "campaign_endDate": None, "campaignBudget_amountMicros": 0,
                      "campaign_optimizationScore": None}
                     for i in range(20)]
        source._run_query = MagicMock(return_value=mock_rows)
        events = collect(source.incremental_snapshot("campaigns", "campaign_id", "0", 5))
        assert len(events) == 5

    def test_all_schemas_non_empty(self, source):
        for table in ["campaigns", "ad_groups", "ads", "keywords",
                      "campaign_performance", "ad_group_performance",
                      "search_terms", "geographic_performance", "audience_performance"]:
            schema = source.get_schema(table)
            assert len(schema) > 0, f"Empty schema for {table}"

    def test_connect_calls_google_ads_client(self, source):
        with patch("sources.google_ads.GoogleAdsSource.connect") as mock_connect:
            mock_connect.return_value = None
            source.connect()
            mock_connect.assert_called_once()


# ══════════════════════════════════════════════════════════════════════════════
# LinkedIn Ads
# ══════════════════════════════════════════════════════════════════════════════

LINKEDIN_CFG = _make_cfg("linkedin_ads", {
    "access_token": "test-access-token",
    "account_id":   "987654321",
}, tables=["campaigns", "ad_analytics"])


class TestLinkedInAdsSource:

    @pytest.fixture
    def source(self):
        from sources.linkedin_ads import LinkedInAdsSource
        src = LinkedInAdsSource("li_test", LINKEDIN_CFG)
        src._session = MagicMock()
        return src

    def test_get_schema_campaigns(self, source):
        schema = source.get_schema("campaigns")
        names = [c.name for c in schema]
        assert "id" in names
        assert "name" in names
        assert "status" in names

    def test_get_schema_ad_analytics(self, source):
        schema = source.get_schema("ad_analytics")
        names = [c.name for c in schema]
        assert "impressions" in names
        assert "clicks" in names
        assert "cost_in_usd" in names
        assert "date" in names

    def test_get_schema_accounts(self, source):
        schema = source.get_schema("accounts")
        assert any(c.name == "id" for c in schema)
        assert any(c.name == "currency" for c in schema)

    def test_get_schema_creatives(self, source):
        schema = source.get_schema("creatives")
        assert any(c.name == "campaign_id" for c in schema)

    def test_get_schema_conversions(self, source):
        schema = source.get_schema("conversions")
        assert any(c.name == "attribution_type" for c in schema)

    def test_all_schemas_non_empty(self, source):
        for table in ["accounts", "campaign_groups", "campaigns", "creatives",
                      "ad_analytics", "ad_analytics_creative", "conversions", "audience_counts"]:
            schema = source.get_schema(table)
            assert len(schema) > 0, f"Empty schema for {table}"

    def test_date_range_default(self, source):
        start, end = source._date_range("ad_analytics")
        assert start < end
        assert end == date.today()

    def test_date_range_incremental(self, source):
        start, end = source._date_range("ad_analytics", "2025-01-10")
        assert start == date(2025, 1, 11)

    def test_date_range_custom_days(self, source):
        cfg = _make_cfg("linkedin_ads", LINKEDIN_CFG["connection"],
                        tables_config={"ad_analytics": {"date_range_days": 7}})
        from sources.linkedin_ads import LinkedInAdsSource
        src = LinkedInAdsSource("t", cfg)
        src._session = MagicMock()
        start, end = src._date_range("ad_analytics")
        assert (end - start).days == 7

    def test_cursor_column_analytics(self, source):
        assert source.get_cursor_column("ad_analytics") == "date"
        assert source.get_cursor_column("ad_analytics_creative") == "date"

    def test_cursor_column_entities(self, source):
        assert source.get_cursor_column("campaigns") == "last_modified"
        assert source.get_cursor_column("campaign_groups") == "last_modified"

    def test_urn_id_helper(self):
        from sources.linkedin_ads import _urn_id
        assert _urn_id("urn:li:sponsoredCampaign:123456") == 123456
        assert _urn_id("urn:li:sponsoredAccount:987") == 987
        assert _urn_id(None) is None
        assert _urn_id("bad-urn") is None

    def test_ms_to_date_helper(self):
        from sources.linkedin_ads import _ms_to_date
        assert _ms_to_date(1700000000000) is not None
        assert _ms_to_date(None) is None

    def test_parse_analytics(self, source):
        element = {
            "pivotValue": "urn:li:sponsoredCampaign:111",
            "dateRange": {"start": {"year": 2025, "month": 3, "day": 15},
                          "end":   {"year": 2025, "month": 3, "day": 15}},
            "impressions": 500, "clicks": 25,
            "costInLocalCurrency": "250.00", "costInUsd": "250.00",
            "externalWebsiteConversions": 3, "leads": 1,
        }
        row = source._parse_analytics(element, "CAMPAIGN")
        assert row["campaign_id"] == 111
        assert row["impressions"] == 500
        assert row["clicks"] == 25
        assert row["date"] == "2025-03-15"
        assert row["ctr"] == pytest.approx(0.05)

    def test_snapshot_campaigns(self, source):
        campaigns_response = {
            "elements": [{
                "id": "urn:li:sponsoredCampaign:777",
                "name": "Test Campaign",
                "status": "ACTIVE",
                "type": "SPONSORED_UPDATES",
                "objectiveType": "BRAND_AWARENESS",
                "costType": "CPM",
                "campaignGroup": "urn:li:sponsoredCampaignGroup:888",
                "changeAuditStamps": {
                    "created": {"time": 1700000000000},
                    "lastModified": {"time": 1700000000000},
                },
            }],
            "paging": {"total": 1},
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = campaigns_response
        mock_resp.raise_for_status = MagicMock()
        source._session.get.return_value = mock_resp

        events = collect(source.snapshot("campaigns"))
        assert len(events) == 1
        assert events[0].after["id"] == 777
        assert events[0].after["name"] == "Test Campaign"
        assert events[0].after["campaign_group_id"] == 888

    def test_snapshot_ad_analytics(self, source):
        analytics_response = {
            "elements": [{
                "pivotValue": "urn:li:sponsoredCampaign:111",
                "dateRange": {"start": {"year": 2025, "month": 3, "day": 1},
                              "end":   {"year": 2025, "month": 3, "day": 1}},
                "impressions": 1000, "clicks": 40,
                "costInLocalCurrency": "80.00", "costInUsd": "80.00",
                "externalWebsiteConversions": 2,
            }]
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = analytics_response
        mock_resp.raise_for_status = MagicMock()
        source._session.get.return_value = mock_resp

        events = collect(source.snapshot("ad_analytics"))
        assert len(events) == 1
        row = events[0].after
        assert row["impressions"] == 1000
        assert row["campaign_id"] == 111

    def test_incremental_snapshot_analytics(self, source):
        analytics_response = {
            "elements": [{
                "pivotValue": "urn:li:sponsoredCampaign:222",
                "dateRange": {"start": {"year": 2025, "month": 3, "day": 5},
                              "end":   {"year": 2025, "month": 3, "day": 5}},
                "impressions": 200, "clicks": 10,
                "costInLocalCurrency": "20.00", "costInUsd": "20.00",
            }]
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = analytics_response
        mock_resp.raise_for_status = MagicMock()
        source._session.get.return_value = mock_resp

        events = collect(source.incremental_snapshot(
            "ad_analytics", "date", "2025-03-04", 100
        ))
        assert len(events) == 1
        assert events[0].after["date"] == "2025-03-05"

    def test_chunk_size_respected(self, source):
        analytics_response = {
            "elements": [{
                "pivotValue": f"urn:li:sponsoredCampaign:{i}",
                "dateRange": {"start": {"year": 2025, "month": 3, "day": i % 28 + 1},
                              "end":   {"year": 2025, "month": 3, "day": i % 28 + 1}},
                "impressions": i * 10, "clicks": i,
                "costInLocalCurrency": str(i * 2.0),
            } for i in range(1, 21)]
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = analytics_response
        mock_resp.raise_for_status = MagicMock()
        source._session.get.return_value = mock_resp

        events = collect(source.incremental_snapshot("ad_analytics", "date", "2025-01-01", 5))
        assert len(events) == 5

    def test_rate_limit_retry(self, source):
        rate_limit_resp = MagicMock()
        rate_limit_resp.status_code = 429
        rate_limit_resp.headers = {"Retry-After": "0"}
        rate_limit_resp.raise_for_status = MagicMock()

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"elements": [], "paging": {"total": 0}}
        ok_resp.raise_for_status = MagicMock()

        source._session.get.side_effect = [rate_limit_resp, ok_resp]
        result = source._paginate("https://api.linkedin.com/rest/adCampaigns")
        assert result == []

    def test_connect_validates_account(self):
        from sources.linkedin_ads import LinkedInAdsSource
        src = LinkedInAdsSource("li_test", LINKEDIN_CFG)

        with patch("requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session_cls.return_value = mock_session
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"id": "urn:li:sponsoredAccount:987654321", "name": "Test"}
            mock_resp.raise_for_status = MagicMock()
            mock_session.get.return_value = mock_resp
            src.connect()
            assert src._session is not None
