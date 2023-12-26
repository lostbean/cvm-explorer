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

  def deriveMetrics(diario_hist_df, target_year_month, num_months) when num_months >= 0 do
    date_col = "DT_COMPTC"
    id_col = "CNPJ_FUNDO"
    reference_year_month = YM.add(target_year_month, -num_months)

    base =
      diario_hist_df
      |> DF.mutate(year_month: 100 * year(col(^date_col)) + month(col(^date_col)))

    dates =
      base
      |> DF.group_by([id_col, "year_month"])
      |> DF.summarise(first_date: min(col(^date_col)), last_date: max(col(^date_col)))
      |> DF.select([id_col, "first_date", "last_date"])

    values = DF.select(base, [id_col, date_col, "VL_QUOTA"])

    monthly_stats =
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

    d_target_ym =
      DF.filter(monthly_stats, year_month == ^target_year_month)
      |> DF.select([id_col, "last_date", "last"])

    d_ref_ym =
      DF.filter(monthly_stats, year_month == ^reference_year_month)
      |> DF.select([id_col, "first_date", "first"])

    full_stats =
      DF.join(d_target_ym, d_ref_ym)
      |> DF.mutate(%{
        "year_month" => 100 * year(col("last_date")) + month(col("last_date")),
        "m#{^num_months}_value_diff_pct" => (last - first) / first
      })

    full_stats
    |> DF.rename(first: "m#{num_months}_value", last: "ref_value")
    |> DF.select([id_col, "m#{num_months}_value_diff_pct", "m#{num_months}_value", "ref_value"])
  end
end
