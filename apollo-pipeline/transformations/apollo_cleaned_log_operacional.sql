-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Resumo da limpeza: operacional
-- MAGIC Compara linhas e nulos de cada tabela entre `raw` e `cleaned`. Fonte da pipeline `Apollo_BI`.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW workspace.cleaned.cleaning_log
COMMENT 'Resumo da limpeza por tabela: linhas e nulos antes e depois dos tratamentos'
AS
SELECT
  tabela,
  linhas_raw,
  linhas_cleaned,
  linhas_raw - linhas_cleaned AS linhas_removidas,
  nulos_raw,
  nulos_cleaned,
  current_timestamp() AS executado_em
FROM (
  SELECT
    'dim_company' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_company) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_company) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(company_key IS NULL AS INT) + CAST(company_name IS NULL AS INT) + CAST(cnpj IS NULL AS INT) + CAST(is_active IS NULL AS INT) + CAST(created_at IS NULL AS INT)), 0) FROM workspace.raw.dim_company) AS nulos_raw,
    (SELECT coalesce(sum(CAST(company_key IS NULL AS INT) + CAST(company_name IS NULL AS INT) + CAST(cnpj IS NULL AS INT) + CAST(is_active IS NULL AS INT) + CAST(created_at IS NULL AS INT)), 0) FROM workspace.cleaned.dim_company) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_segment' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_segment) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_segment) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(segment_key IS NULL AS INT) + CAST(segment_name IS NULL AS INT)), 0) FROM workspace.raw.dim_segment) AS nulos_raw,
    (SELECT coalesce(sum(CAST(segment_key IS NULL AS INT) + CAST(segment_name IS NULL AS INT)), 0) FROM workspace.cleaned.dim_segment) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_company_unit' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_company_unit) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_company_unit) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(company_unit_key IS NULL AS INT) + CAST(company_key IS NULL AS INT) + CAST(segment_key IS NULL AS INT) + CAST(unit_name IS NULL AS INT) + CAST(city IS NULL AS INT) + CAST(state IS NULL AS INT) + CAST(kwp_total IS NULL AS INT) + CAST(is_active IS NULL AS INT)), 0) FROM workspace.raw.dim_company_unit) AS nulos_raw,
    (SELECT coalesce(sum(CAST(company_unit_key IS NULL AS INT) + CAST(company_key IS NULL AS INT) + CAST(segment_key IS NULL AS INT) + CAST(unit_name IS NULL AS INT) + CAST(city IS NULL AS INT) + CAST(state IS NULL AS INT) + CAST(kwp_total IS NULL AS INT) + CAST(is_active IS NULL AS INT)), 0) FROM workspace.cleaned.dim_company_unit) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_panel_model' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_panel_model) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_panel_model) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(panel_model_key IS NULL AS INT) + CAST(manufacturer IS NULL AS INT) + CAST(model IS NULL AS INT) + CAST(pmax_w IS NULL AS INT) + CAST(efficiency_pct IS NULL AS INT) + CAST(warranty_years IS NULL AS INT)), 0) FROM workspace.raw.dim_panel_model) AS nulos_raw,
    (SELECT coalesce(sum(CAST(panel_model_key IS NULL AS INT) + CAST(manufacturer IS NULL AS INT) + CAST(model IS NULL AS INT) + CAST(pmax_w IS NULL AS INT) + CAST(efficiency_pct IS NULL AS INT) + CAST(warranty_years IS NULL AS INT)), 0) FROM workspace.cleaned.dim_panel_model) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_inverter' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_inverter) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_inverter) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(inverter_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(code IS NULL AS INT) + CAST(brand IS NULL AS INT) + CAST(model IS NULL AS INT) + CAST(rated_power_kw IS NULL AS INT) + CAST(mppt_count IS NULL AS INT) + CAST(strings_per_mppt IS NULL AS INT) + CAST(status IS NULL AS INT) + CAST(installed_at IS NULL AS INT)), 0) FROM workspace.raw.dim_inverter) AS nulos_raw,
    (SELECT coalesce(sum(CAST(inverter_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(code IS NULL AS INT) + CAST(brand IS NULL AS INT) + CAST(model IS NULL AS INT) + CAST(rated_power_kw IS NULL AS INT) + CAST(mppt_count IS NULL AS INT) + CAST(strings_per_mppt IS NULL AS INT) + CAST(status IS NULL AS INT) + CAST(installed_at IS NULL AS INT)), 0) FROM workspace.cleaned.dim_inverter) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_string' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_string) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_string) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(string_key IS NULL AS INT) + CAST(inverter_key IS NULL AS INT) + CAST(panel_model_key IS NULL AS INT) + CAST(code IS NULL AS INT) + CAST(mppt_number IS NULL AS INT) + CAST(entry_number IS NULL AS INT) + CAST(invoice_number IS NULL AS INT) + CAST(acquisition_dt IS NULL AS INT) + CAST(unit_cost IS NULL AS INT) + CAST(purchased_qty IS NULL AS INT) + CAST(installed_panels IS NULL AS INT) + CAST(nominal_power_wp IS NULL AS INT) + CAST(status IS NULL AS INT)), 0) FROM workspace.raw.dim_string) AS nulos_raw,
    (SELECT coalesce(sum(CAST(string_key IS NULL AS INT) + CAST(inverter_key IS NULL AS INT) + CAST(panel_model_key IS NULL AS INT) + CAST(code IS NULL AS INT) + CAST(mppt_number IS NULL AS INT) + CAST(entry_number IS NULL AS INT) + CAST(invoice_number IS NULL AS INT) + CAST(acquisition_dt IS NULL AS INT) + CAST(unit_cost IS NULL AS INT) + CAST(purchased_qty IS NULL AS INT) + CAST(installed_panels IS NULL AS INT) + CAST(nominal_power_wp IS NULL AS INT) + CAST(status IS NULL AS INT)), 0) FROM workspace.cleaned.dim_string) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_panel' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_panel) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_panel) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(panel_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(serial_number IS NULL AS INT) + CAST(barcode IS NULL AS INT) + CAST(operating_stats IS NULL AS INT) + CAST(installation_dt IS NULL AS INT) + CAST(deactivated_at IS NULL AS INT)), 0) FROM workspace.raw.dim_panel) AS nulos_raw,
    (SELECT coalesce(sum(CAST(panel_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(serial_number IS NULL AS INT) + CAST(barcode IS NULL AS INT) + CAST(operating_stats IS NULL AS INT) + CAST(installation_dt IS NULL AS INT) + CAST(deactivated_at IS NULL AS INT)), 0) FROM workspace.cleaned.dim_panel) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_employee' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_employee) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_employee) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(employee_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(job_role IS NULL AS INT) + CAST(is_active IS NULL AS INT)), 0) FROM workspace.raw.dim_employee) AS nulos_raw,
    (SELECT coalesce(sum(CAST(employee_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(job_role IS NULL AS INT) + CAST(is_active IS NULL AS INT)), 0) FROM workspace.cleaned.dim_employee) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_date' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_date) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_date) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(full_date IS NULL AS INT) + CAST(day_number IS NULL AS INT) + CAST(month_number IS NULL AS INT) + CAST(month_name IS NULL AS INT) + CAST(quarter_number IS NULL AS INT) + CAST(year_number IS NULL AS INT) + CAST(is_weekend IS NULL AS INT)), 0) FROM workspace.raw.dim_date) AS nulos_raw,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(full_date IS NULL AS INT) + CAST(day_number IS NULL AS INT) + CAST(month_number IS NULL AS INT) + CAST(month_name IS NULL AS INT) + CAST(quarter_number IS NULL AS INT) + CAST(year_number IS NULL AS INT) + CAST(is_weekend IS NULL AS INT)), 0) FROM workspace.cleaned.dim_date) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_maintenance_type' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_maintenance_type) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_maintenance_type) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(maintenance_type_key IS NULL AS INT) + CAST(type_name IS NULL AS INT) + CAST(default_interval_days IS NULL AS INT)), 0) FROM workspace.raw.dim_maintenance_type) AS nulos_raw,
    (SELECT coalesce(sum(CAST(maintenance_type_key IS NULL AS INT) + CAST(type_name IS NULL AS INT) + CAST(default_interval_days IS NULL AS INT)), 0) FROM workspace.cleaned.dim_maintenance_type) AS nulos_cleaned
  UNION ALL
  SELECT
    'dim_part' AS tabela,
    (SELECT count(*) FROM workspace.raw.dim_part) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.dim_part) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(part_key IS NULL AS INT) + CAST(sku IS NULL AS INT) + CAST(part_name IS NULL AS INT) + CAST(manufacturer IS NULL AS INT)), 0) FROM workspace.raw.dim_part) AS nulos_raw,
    (SELECT coalesce(sum(CAST(part_key IS NULL AS INT) + CAST(sku IS NULL AS INT) + CAST(part_name IS NULL AS INT) + CAST(manufacturer IS NULL AS INT)), 0) FROM workspace.cleaned.dim_part) AS nulos_cleaned
  UNION ALL
  SELECT
    'fact_string_daily_performance' AS tabela,
    (SELECT count(*) FROM workspace.raw.fact_string_daily_performance) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.fact_string_daily_performance) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(inverter_key IS NULL AS INT) + CAST(measurement_date IS NULL AS INT) + CAST(generated_energy_kwh IS NULL AS INT) + CAST(peak_power_w IS NULL AS INT) + CAST(nominal_power_wp IS NULL AS INT) + CAST(peak_to_nominal_ratio IS NULL AS INT) + CAST(fault_count IS NULL AS INT) + CAST(measurement_count IS NULL AS INT) + CAST(running_total_energy_kwh IS NULL AS INT) + CAST(inverter_yield_rank IS NULL AS INT) + CAST(energy_moving_average_7d IS NULL AS INT)), 0) FROM workspace.raw.fact_string_daily_performance) AS nulos_raw,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(inverter_key IS NULL AS INT) + CAST(measurement_date IS NULL AS INT) + CAST(generated_energy_kwh IS NULL AS INT) + CAST(peak_power_w IS NULL AS INT) + CAST(nominal_power_wp IS NULL AS INT) + CAST(peak_to_nominal_ratio IS NULL AS INT) + CAST(fault_count IS NULL AS INT) + CAST(measurement_count IS NULL AS INT) + CAST(running_total_energy_kwh IS NULL AS INT) + CAST(inverter_yield_rank IS NULL AS INT) + CAST(energy_moving_average_7d IS NULL AS INT)), 0) FROM workspace.cleaned.fact_string_daily_performance) AS nulos_cleaned
  UNION ALL
  SELECT
    'fact_string_health' AS tabela,
    (SELECT count(*) FROM workspace.raw.fact_string_health) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.fact_string_health) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(inverter_key IS NULL AS INT) + CAST(calculated_at IS NULL AS INT) + CAST(window_days IS NULL AS INT) + CAST(sibling_ratio IS NULL AS INT) + CAST(nominal_ratio IS NULL AS INT) + CAST(warning_penalty IS NULL AS INT) + CAST(health_score IS NULL AS INT) + CAST(health_moving_average_7 IS NULL AS INT) + CAST(health_delta IS NULL AS INT)), 0) FROM workspace.raw.fact_string_health) AS nulos_raw,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(inverter_key IS NULL AS INT) + CAST(calculated_at IS NULL AS INT) + CAST(window_days IS NULL AS INT) + CAST(sibling_ratio IS NULL AS INT) + CAST(nominal_ratio IS NULL AS INT) + CAST(warning_penalty IS NULL AS INT) + CAST(health_score IS NULL AS INT) + CAST(health_moving_average_7 IS NULL AS INT) + CAST(health_delta IS NULL AS INT)), 0) FROM workspace.cleaned.fact_string_health) AS nulos_cleaned
  UNION ALL
  SELECT
    'fact_maintenance' AS tabela,
    (SELECT count(*) FROM workspace.raw.fact_maintenance) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.fact_maintenance) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(opening_date_key IS NULL AS INT) + CAST(maintenance_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(warning_key IS NULL AS INT) + CAST(warning_type IS NULL AS INT) + CAST(priority IS NULL AS INT) + CAST(maintenance_type_key IS NULL AS INT) + CAST(technician_key IS NULL AS INT) + CAST(created_by_key IS NULL AS INT) + CAST(cancelled_by_key IS NULL AS INT) + CAST(opening_dt IS NULL AS INT) + CAST(due_date IS NULL AS INT) + CAST(concluded_at IS NULL AS INT) + CAST(cancelled_at IS NULL AS INT) + CAST(maintenance_status IS NULL AS INT) + CAST(calculated_status IS NULL AS INT) + CAST(estimated_cost IS NULL AS INT) + CAST(labor_cost IS NULL AS INT) + CAST(parts_cost IS NULL AS INT) + CAST(total_cost IS NULL AS INT) + CAST(resolution_days IS NULL AS INT) + CAST(unit_avg_resolution_days IS NULL AS INT) + CAST(unit_resolution_rank IS NULL AS INT) + CAST(unit_running_maintenance_cost IS NULL AS INT)), 0) FROM workspace.raw.fact_maintenance) AS nulos_raw,
    (SELECT coalesce(sum(CAST(opening_date_key IS NULL AS INT) + CAST(maintenance_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(warning_key IS NULL AS INT) + CAST(warning_type IS NULL AS INT) + CAST(priority IS NULL AS INT) + CAST(maintenance_type_key IS NULL AS INT) + CAST(technician_key IS NULL AS INT) + CAST(created_by_key IS NULL AS INT) + CAST(cancelled_by_key IS NULL AS INT) + CAST(opening_dt IS NULL AS INT) + CAST(due_date IS NULL AS INT) + CAST(concluded_at IS NULL AS INT) + CAST(cancelled_at IS NULL AS INT) + CAST(maintenance_status IS NULL AS INT) + CAST(calculated_status IS NULL AS INT) + CAST(estimated_cost IS NULL AS INT) + CAST(labor_cost IS NULL AS INT) + CAST(parts_cost IS NULL AS INT) + CAST(total_cost IS NULL AS INT) + CAST(resolution_days IS NULL AS INT) + CAST(unit_avg_resolution_days IS NULL AS INT) + CAST(unit_resolution_rank IS NULL AS INT) + CAST(unit_running_maintenance_cost IS NULL AS INT)), 0) FROM workspace.cleaned.fact_maintenance) AS nulos_cleaned
  UNION ALL
  SELECT
    'fact_part_consumption' AS tabela,
    (SELECT count(*) FROM workspace.raw.fact_part_consumption) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.fact_part_consumption) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(maintenance_key IS NULL AS INT) + CAST(part_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(quantity IS NULL AS INT) + CAST(unit_cost IS NULL AS INT) + CAST(total_cost IS NULL AS INT) + CAST(running_quantity IS NULL AS INT)), 0) FROM workspace.raw.fact_part_consumption) AS nulos_raw,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(maintenance_key IS NULL AS INT) + CAST(part_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(quantity IS NULL AS INT) + CAST(unit_cost IS NULL AS INT) + CAST(total_cost IS NULL AS INT) + CAST(running_quantity IS NULL AS INT)), 0) FROM workspace.cleaned.fact_part_consumption) AS nulos_cleaned
  UNION ALL
  SELECT
    'fact_daily_active_user' AS tabela,
    (SELECT count(*) FROM workspace.raw.fact_daily_active_user) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.fact_daily_active_user) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(company_key IS NULL AS INT) + CAST(date_key IS NULL AS INT) + CAST(access_date IS NULL AS INT) + CAST(daily_active_users IS NULL AS INT) + CAST(total_sessions IS NULL AS INT) + CAST(rolling_7d_active_user_sum IS NULL AS INT)), 0) FROM workspace.raw.fact_daily_active_user) AS nulos_raw,
    (SELECT coalesce(sum(CAST(company_key IS NULL AS INT) + CAST(date_key IS NULL AS INT) + CAST(access_date IS NULL AS INT) + CAST(daily_active_users IS NULL AS INT) + CAST(total_sessions IS NULL AS INT) + CAST(rolling_7d_active_user_sum IS NULL AS INT)), 0) FROM workspace.cleaned.fact_daily_active_user) AS nulos_cleaned
  UNION ALL
  SELECT
    'fact_warning' AS tabela,
    (SELECT count(*) FROM workspace.raw.fact_warning) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.fact_warning) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(warning_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(inverter_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(panel_key IS NULL AS INT) + CAST(warning_type IS NULL AS INT) + CAST(severity IS NULL AS INT) + CAST(status IS NULL AS INT) + CAST(generation_dt IS NULL AS INT) + CAST(resolved_at IS NULL AS INT) + CAST(maintenance_key IS NULL AS INT) + CAST(maintenance_status IS NULL AS INT) + CAST(maintenance_opening_dt IS NULL AS INT) + CAST(hours_to_maintenance IS NULL AS INT) + CAST(resolution_days IS NULL AS INT)), 0) FROM workspace.raw.fact_warning) AS nulos_raw,
    (SELECT coalesce(sum(CAST(date_key IS NULL AS INT) + CAST(warning_key IS NULL AS INT) + CAST(company_unit_key IS NULL AS INT) + CAST(inverter_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(panel_key IS NULL AS INT) + CAST(warning_type IS NULL AS INT) + CAST(severity IS NULL AS INT) + CAST(status IS NULL AS INT) + CAST(generation_dt IS NULL AS INT) + CAST(resolved_at IS NULL AS INT) + CAST(maintenance_key IS NULL AS INT) + CAST(maintenance_status IS NULL AS INT) + CAST(maintenance_opening_dt IS NULL AS INT) + CAST(hours_to_maintenance IS NULL AS INT) + CAST(resolution_days IS NULL AS INT)), 0) FROM workspace.cleaned.fact_warning) AS nulos_cleaned
  UNION ALL
  SELECT
    'fact_relocation' AS tabela,
    (SELECT count(*) FROM workspace.raw.fact_relocation) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.fact_relocation) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(relocation_scope IS NULL AS INT) + CAST(relocation_key IS NULL AS INT) + CAST(origin_unit_key IS NULL AS INT) + CAST(destination_unit_key IS NULL AS INT) + CAST(destination_company_key IS NULL AS INT) + CAST(segment_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(panel_key IS NULL AS INT) + CAST(panel_quantity IS NULL AS INT) + CAST(status IS NULL AS INT) + CAST(suggested_at IS NULL AS INT) + CAST(reviewed_at IS NULL AS INT) + CAST(review_days IS NULL AS INT)), 0) FROM workspace.raw.fact_relocation) AS nulos_raw,
    (SELECT coalesce(sum(CAST(relocation_scope IS NULL AS INT) + CAST(relocation_key IS NULL AS INT) + CAST(origin_unit_key IS NULL AS INT) + CAST(destination_unit_key IS NULL AS INT) + CAST(destination_company_key IS NULL AS INT) + CAST(segment_key IS NULL AS INT) + CAST(string_key IS NULL AS INT) + CAST(panel_key IS NULL AS INT) + CAST(panel_quantity IS NULL AS INT) + CAST(status IS NULL AS INT) + CAST(suggested_at IS NULL AS INT) + CAST(reviewed_at IS NULL AS INT) + CAST(review_days IS NULL AS INT)), 0) FROM workspace.cleaned.fact_relocation) AS nulos_cleaned
  UNION ALL
  SELECT
    'fact_stock' AS tabela,
    (SELECT count(*) FROM workspace.raw.fact_stock) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.fact_stock) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(company_unit_key IS NULL AS INT) + CAST(part_key IS NULL AS INT) + CAST(available_qtt IS NULL AS INT) + CAST(minimum_qtt IS NULL AS INT) + CAST(unit_cost IS NULL AS INT) + CAST(stock_value IS NULL AS INT) + CAST(below_minimum IS NULL AS INT) + CAST(updated_at IS NULL AS INT)), 0) FROM workspace.raw.fact_stock) AS nulos_raw,
    (SELECT coalesce(sum(CAST(company_unit_key IS NULL AS INT) + CAST(part_key IS NULL AS INT) + CAST(available_qtt IS NULL AS INT) + CAST(minimum_qtt IS NULL AS INT) + CAST(unit_cost IS NULL AS INT) + CAST(stock_value IS NULL AS INT) + CAST(below_minimum IS NULL AS INT) + CAST(updated_at IS NULL AS INT)), 0) FROM workspace.cleaned.fact_stock) AS nulos_cleaned
  UNION ALL
  SELECT
    'vw_maintenance_chain' AS tabela,
    (SELECT count(*) FROM workspace.raw.vw_maintenance_chain) AS linhas_raw,
    (SELECT count(*) FROM workspace.cleaned.vw_maintenance_chain) AS linhas_cleaned,
    (SELECT coalesce(sum(CAST(id IS NULL AS INT) + CAST(parent_maintenance_id IS NULL AS INT) + CAST(warning_id IS NULL AS INT) + CAST(maintenance_status IS NULL AS INT) + CAST(opening_dt IS NULL AS INT) + CAST(chain_level IS NULL AS INT) + CAST(path IS NULL AS INT) + CAST(string_id IS NULL AS INT)), 0) FROM workspace.raw.vw_maintenance_chain) AS nulos_raw,
    (SELECT coalesce(sum(CAST(id IS NULL AS INT) + CAST(parent_maintenance_id IS NULL AS INT) + CAST(warning_id IS NULL AS INT) + CAST(maintenance_status IS NULL AS INT) + CAST(opening_dt IS NULL AS INT) + CAST(chain_level IS NULL AS INT) + CAST(path IS NULL AS INT) + CAST(string_id IS NULL AS INT)), 0) FROM workspace.cleaned.vw_maintenance_chain) AS nulos_cleaned
);
