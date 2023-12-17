defmodule CvmExplorer.DiarioFI do
  require Explorer.DataFrame, as: DF

  @col_mapping [
    {"CAPTC_DIA", {:f, 64}},
    {"CNPJ_FUNDO", :string},
    {"DT_COMPTC", :date},
    {"NR_COTST", :integer},
    {"RESG_DIA", {:f, 64}},
    {"TP_FUNDO", :string},
    {"VL_PATRIM_LIQ", {:f, 64}},
    {"VL_QUOTA", {:f, 64}},
    {"VL_TOTAL", {:f, 64}}
  ]
  @explorer_opts [
    delimiter: ";",
    dtypes: @col_mapping,
    nil_values: [""]
  ]

  def dataframe(month, year) do
    year_month = year * 100 + month
    filename = "inf_diario_fi_#{year_month}.csv"
    {:ok, cad_fi} = DF.from_csv(filename, @explorer_opts)
    cad_fi
  end

  def download(month, year) do
    year_month = year * 100 + month

    diario_url =
      "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_#{year_month}.zip"

    HTTPStreamer.get(diario_url)
    |> Zstream.unzip()
    |> Enum.reduce(%{}, fn
      {:entry, %Zstream.Entry{name: file_name}}, _ ->
        {initial_acc, collector_fun} = Collectable.into(File.stream!(file_name))
        %{acc: initial_acc, collector_fun: collector_fun}

      {:data, :eof}, %{acc: acc, collector_fun: collector_fun} ->
        collector_fun.(acc, :done)

      {:data, [data]}, %{acc: acc, collector_fun: collector_fun} ->
        updated_acc = collector_fun.(acc, {:cont, data})
        %{acc: updated_acc, collector_fun: collector_fun}
    end)
    |> Stream.map(&:iconv.convert("ISO-8859-1", "utf-8", &1))
    |> Stream.run()
  end
end
