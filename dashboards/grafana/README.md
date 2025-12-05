# Grafana Dashboards Setup Guide

## Overview
Grafana is used for real-time monitoring of the GlobalMart platform.

## Access
- URL: http://localhost:3000
- Default credentials: admin/admin (change on first login)

## Data Sources

### Configured Automatically
The following data sources are provisioned automatically on startup:

1. **Prometheus** (System Metrics)
   - URL: http://prometheus:9090
   - Purpose: System health, Kafka metrics, API performance

2. **PostgreSQL** (Data Warehouse)
   - Database: globalmart_warehouse
   - Purpose: Historical sales data, analytical queries

### Manual Configuration Required

**MongoDB** (Real-Time Metrics)
Since MongoDB data source requires a plugin, follow these steps:

1. Install MongoDB plugin:
   ```bash
   docker exec -it globalmart-grafana grafana-cli plugins install grafana-mongodb-datasource
   docker restart globalmart-grafana
   ```

2. Add MongoDB data source in Grafana UI:
   - Go to Configuration > Data Sources > Add data source
   - Search for "MongoDB"
   - Configure:
     - URL: mongodb://mongodb:27017
     - Database: globalmart_realtime
     - Username: globalmart_user
     - Password: (from .env file)

## Dashboards

### Planned Dashboards

1. **System Health Dashboard**
   - CPU, memory, disk usage
   - Kafka broker status
   - Service uptime

2. **Data Ingestion Dashboard**
   - Events per second by topic
   - Kafka lag
   - Producer success/failure rates

3. **Stream Processing Dashboard**
   - Processing latency
   - Records processed per batch
   - Active sessions
   - Anomalies detected

4. **Real-Time Business Metrics**
   - Sales per minute/hour
   - Current GMV
   - Top selling products
   - Sales by region

5. **Alerts Dashboard**
   - Low stock alerts
   - Anomaly alerts
   - System health alerts

## Creating Dashboards

### From PostgreSQL
Example query for total sales:
```sql
SELECT
    date_trunc('hour', transaction_timestamp) as time,
    SUM(total_amount) as total_sales
FROM fact_sales
WHERE transaction_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY time
ORDER BY time
```

### From Prometheus
Example query for system metrics:
```
rate(process_cpu_seconds_total[5m])
```

## Dashboard Provisioning

To automatically provision dashboards:
1. Export dashboard JSON from Grafana UI
2. Save to `dashboards/grafana/provisioning/dashboards/`
3. Create dashboard config file in same directory
4. Restart Grafana

Example dashboard config (`dashboards.yml`):
```yaml
apiVersion: 1

providers:
  - name: 'GlobalMart'
    orgId: 1
    folder: 'GlobalMart'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 10
    allowUiUpdates: true
    options:
      path: /etc/grafana/provisioning/dashboards
```

## Troubleshooting

### Cannot connect to data sources
- Ensure all services are running: `./infrastructure/scripts/health_check.sh`
- Check network connectivity: All services must be on `globalmart-network`
- Verify credentials in `.env` file

### Dashboards not showing data
- Check data source configuration
- Verify queries return data
- Check time range selection

### MongoDB plugin issues
- Restart Grafana after plugin installation
- Check Grafana logs: `docker logs globalmart-grafana`

## Next Steps

1. Configure MongoDB data source manually (after plugin installation)
2. Create dashboards using the Grafana UI
3. Export and save dashboard JSONs for version control
4. Set up alerting rules for critical metrics
