defmodule CvmExplorer.EconomicalIndicators do
  require Explorer.DataFrame, as: DF
  @indicator_map %{cdi: 4391}

  @col_mapping [
    {"data", :date}
  ]
  @explorer_opts [
    delimiter: ";",
    dtypes: @col_mapping,
    nil_values: [""]
  ]

  def url_indicator_by_code(indicator) do
    bcb_code = Map.get(@indicator_map, indicator)

    unless bcb_code do
      raise "indicator #{indicator} not mapped, available indicators: #{inspect(Map.keys(@indicator_map))}"
    end

    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.#{bcb_code}/dados?formato=csv"
  end

  defp get_filename(indicator) do
    "indicator_#{Atom.to_string(indicator)}.csv"
  end

  def download(indicator) do
    filename = get_filename(indicator)

    url_indicator_by_code(indicator)
    |> HTTPStreamer.get()
    |> Stream.map(&:iconv.convert("ISO-8859-1", "utf-8", &1))
    |> Stream.into(File.stream!(filename, encoding: :utf8))
    |> Stream.run()
  end

  def dataframe(indicator) do
    filename = get_filename(indicator)
    date_col = "data"

    if !File.exists?(filename), do: download(indicator)
    {:ok, df} = DF.from_csv(filename, @explorer_opts)

    df
    |> DF.mutate(valor: 0.01 * cast(replace(valor, ",", "."), {:f, 64}))
    |> DF.mutate(year_month: 100 * year(col(^date_col)) + month(col(^date_col)))
  end
end
