defmodule CvmExplorer do
  require Explorer.DataFrame, as: DF
  require CvmExplorer.Utils.YearMonth, as: YM

  @spec getDiarioFIHistory(number()) :: Explorer.DataFrame.t()
  def getDiarioFIHistory(num_months) do
    current_ym = {2023, 11}

    Enum.to_list(-num_months..0)
    |> Enum.map(fn diff ->
      {year, month} = YM.add(current_ym, diff)
      CvmExplorer.DiarioFI.dataframe(month, year)
    end)
    |> DF.concat_rows()
  end

  def monthly_stats(diario_hist_df) do
    date_col = "DT_COMPTC"
    id_col = "CNPJ_FUNDO"

    base =
      diario_hist_df
      |> DF.mutate(year_month: 100 * year(col(^date_col)) + month(col(^date_col)))

    dates =
      base
      |> DF.group_by([id_col, "year_month"])
      |> DF.summarise(first_date: min(col(^date_col)), last_date: max(col(^date_col)))
      |> DF.select([id_col, "first_date", "last_date"])

    values = DF.select(base, [id_col, date_col, "VL_QUOTA"])

    dates
    |> DF.join(DF.rename(values, VL_QUOTA: "first"),
      on: [{id_col, id_col}, {"first_date", date_col}]
    )
    |> DF.join(DF.rename(values, VL_QUOTA: "last"),
      on: [{id_col, id_col}, {"last_date", date_col}]
    )
    |> DF.mutate(
      year_month: 100 * year(col("first_date")) + month(col("first_date")),
      value_diff_pct: (last - first) / first
    )
  end

  @spec deriveMetrics(Explorer.DataFrame.t(), integer(), list() | integer()) ::
          Explorer.DataFrame.t()
  def deriveMetrics(diario_hist_df, target_year_month, num_months) when is_integer(num_months) do
    deriveMetrics(diario_hist_df, target_year_month, [num_months])
  end

  def deriveMetrics(diario_hist_df, target_year_month, num_months) when is_list(num_months) do
    id_col = "CNPJ_FUNDO"
    yms = Enum.map(num_months, &YM.add(target_year_month, -&1))

    monthly_stats = monthly_stats(diario_hist_df)

    yms_str = Enum.map(yms, &to_string(&1))

    DF.filter(monthly_stats, year_month == ^target_year_month or year_month in ^yms)
    |> DF.pivot_wider("year_month", "last", id_columns: [id_col])
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

  def getCDIBenchmark(num_past_months) do
    all_df = CvmExplorer.getDiarioFIHistory(num_past_months)
    cdi_df = CvmExplorer.EconomicalIndicators.dataframe(:cdi)
    monthly_df = CvmExplorer.monthly_stats(all_df)

    count_df =
      DF.join(monthly_df, cdi_df, on: [{"year_month", "year_month"}])
      |> DF.group_by("CNPJ_FUNDO")
      |> DF.summarise(
        count_better: sum(if value_diff_pct > valor, do: 1, else: 0),
        count_worst: sum(if value_diff_pct <= valor, do: 1, else: 0)
      )

    DF.mutate(count_df, ratio: count_better / count_worst)
  end
end
