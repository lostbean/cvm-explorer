defmodule CvmExplorer do
  require Explorer.DataFrame, as: DF
  require CvmExplorer.Utils.YearMonth, as: YM

  @spec getDiarioFIHistory(number()) :: Explorer.DataFrame.t()
  def getDiarioFIHistory(num_months) do
    # TODO: Use the current year and month
    current_ym = {2023, 11}

    Enum.to_list(-num_months..0)
    |> Enum.map(fn diff ->
      {year, month} = YM.add(current_ym, diff)
      CvmExplorer.DiarioFI.dataframe(month, year)
    end)
    |> DF.concat_rows()
    |> DF.rename_with(&String.downcase/1)
  end

  def monthly_stats(diario_hist_df) do
    id_col = "cnpj"
    date_col = "date"
    year_month_col = "year_month"
    share_col = "value_share"
    total_col = "value_total"
    net_total_col = "value_net_total"
    share_diff_col = "value_share_diff_pct"
    num_shareholders_col = "num_shareholders"

    base =
      diario_hist_df
      |> DF.rename(
        cnpj_fundo: id_col,
        dt_comptc: date_col,
        vl_quota: share_col,
        vl_total: total_col,
        vl_patrim_liq: net_total_col,
        nr_cotst: num_shareholders_col
      )
      |> DF.mutate(year_month: 100 * year(col(^date_col)) + month(col(^date_col)))

    dates =
      base
      |> DF.group_by([id_col, year_month_col])
      |> DF.summarise(first_date: min(col(^date_col)), last_date: max(col(^date_col)))
      |> DF.select([id_col, "first_date", "last_date"])

    start_values =
      base
      |> DF.rename(%{"#{share_col}" => "first_#{share_col}"})
      |> DF.select([id_col, date_col, "first_#{share_col}"])

    end_values =
      base

    dates
    |> DF.join(start_values,
      on: [{id_col, id_col}, {"first_date", date_col}]
    )
    |> DF.join(end_values,
      on: [{id_col, id_col}, {"last_date", date_col}]
    )
    |> DF.mutate(
      year_month: 100 * year(col("first_date")) + month(col("first_date")),
      value_share_diff_pct:
        (col(^share_col) - col("first_#{^share_col}")) / col("first_#{^share_col}")
    )
    |> DF.rename(first_date: date_col)
    |> DF.select([
      id_col,
      date_col,
      year_month_col,
      share_col,
      total_col,
      net_total_col,
      num_shareholders_col,
      share_diff_col
    ])
  end

  @spec deriveMetrics(Explorer.DataFrame.t(), integer(), list() | integer()) ::
          Explorer.DataFrame.t()
  def deriveMetrics(diario_hist_df, target_year_month, num_months) when is_integer(num_months) do
    deriveMetrics(diario_hist_df, target_year_month, [num_months])
  end

  def deriveMetrics(diario_hist_df, target_year_month, num_months) when is_list(num_months) do
    id_col = "cnpj"
    yms = Enum.map(num_months, &YM.add(target_year_month, -&1))

    monthly_stats = monthly_stats(diario_hist_df)

    yms_str = Enum.map(yms, &to_string(&1))

    DF.filter(monthly_stats, year_month == ^target_year_month or year_month in ^yms)
    |> DF.pivot_wider("year_month", "value_share", id_columns: [id_col])
    |> DF.rename(%{to_string(target_year_month) => "target_year_month"})
    |> DF.mutate(
      for col <- across(^yms_str) do
        {"total_return_since_#{col.name}", (target_year_month - col) / col}
      end
    )
    |> DF.mutate(
      for col <- across(^yms_str) do
        {"avg_return_since_#{col.name}",
         (target_year_month - col) /
           (YM.diff(^target_year_month, String.to_integer(col.name)) * col)}
      end
    )
  end

  def get_cdi_benchmark(num_past_months) do
    all_df = CvmExplorer.getDiarioFIHistory(num_past_months)
    cdi_df = CvmExplorer.EconomicalIndicators.dataframe(:cdi)
    monthly_df = CvmExplorer.monthly_stats(all_df)

    DF.join(monthly_df, cdi_df, on: [{"year_month", "year_month"}])
    |> DF.group_by("cnpj_fundo")
    |> DF.summarise(
      count_better: sum(if value_diff_pct > valor, do: 1, else: 0),
      count_worst: sum(if value_diff_pct <= valor, do: 1, else: 0)
    )
    |> DF.mutate(ratio: count_better / count_worst)
  end
end
