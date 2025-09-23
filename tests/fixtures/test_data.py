"""
Additional test data and fixtures for comprehensive testing.
"""

from datetime import datetime, timezone


# Error responses for testing failure scenarios
SUMOLOGIC_AUTH_ERROR = {
    'status': 401,
    'message': 'Unauthorized - Invalid credentials'
}

SUMOLOGIC_RATE_LIMIT_ERROR = {
    'status': 429,
    'message': 'Rate limit exceeded'
}

CLOUDZERO_API_ERROR = {
    'error': 'Invalid stream connection ID',
    'code': 400
}

# Sample queries for testing (shortened versions of actual queries)
SAMPLE_CONTINUOUS_QUERY = '''
_index=sumologic_volume _sourceCategory = "sourcecategory_and_tier_volume"
| parse regex "(?<data>\\{[^\\{]+\\})" multi
| json field=data "field","dataTier","sizeInBytes","count" as sourcecategory, dataTier, bytes, count
| where dataTier matches "Continuous"
| where !(sourcecategory matches "*_volume")
| bytes/1Gi as gbytes
| timeslice 1h
| sum(gbytes) as gbytes by _timeslice, sourceCategory, dataTier
| gbytes*20 as credits
'''

SAMPLE_METRICS_QUERY = '''
_index=sumologic_volume _sourceCategory="sourcecategory_metrics_volume" datapoints
| parse regex "\\"(?<sourcecategory>[^\\"]+)\\"\\:\\{\\"dataPoints\\"\\:(?<datapoints>\\d+)\\}" multi
| timeslice 24h
| sum(datapoints) as datapoints by sourcecategory, _timeslice
| ((queryEndTime() - queryStartTime())/(1000*60)) as duration_in_min
| datapoints / duration_in_min as DPM
| DPM/1000 as AvgKDPM
| AvgKDPM *3 as credits
'''

# Test dates for various scenarios
TEST_DATES = {
    'standard': datetime(2025, 9, 22, 12, 0, 0, tzinfo=timezone.utc),
    'start_of_month': datetime(2025, 9, 1, 0, 0, 0, tzinfo=timezone.utc),
    'end_of_month': datetime(2025, 9, 30, 23, 59, 59, tzinfo=timezone.utc),
    'leap_year': datetime(2024, 2, 29, 12, 0, 0, tzinfo=timezone.utc),
}

# Large payload data for chunking tests
LARGE_CBF_RECORDS = []
for i in range(1000):  # Generate 1000 records for chunking tests
    LARGE_CBF_RECORDS.append({
        'time/usage_start': f'2025-09-22T{i % 24:02d}:00:00+00:00',
        'resource/id': f'sourcecategory/test|service|{i}',
        'resource/usage_family': 'Continuous',
        'lineitem/type': 'Usage',
        'lineitem/description': 'Test logs for chunking',
        'resource/service': 'Logs continuous ingest',
        'resource/account': 'test_org_id',
        'resource/region': 'us1',
        'usage/units': 'credits',
        'action/operation': 'ingest',
        'usage/amount': f'{(i * 0.001):.6f}',
        'cost/cost': f'{(i * 0.001 * 0.15):.6f}'
    })