defmodule CvmExplorer do
  @cad_fi "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
  @cad_fi_store_file "cad_fi_utf8.csv"
  @col_mapping [
    {"DT_REG", :date},
    {"DT_CONST", :date},
    {"DT_CANCEL", :date},
    {"DT_INI_SIT", :date},
    {"DT_INI_ATIV", :date},
    {"DT_INI_EXERC", :date},
    {"DT_FIM_EXERC", :date},
    {"DT_INI_CLASSE", :date},
    # diario_fi
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

  def download_cad_fi() do
    HTTPStreamer.get(@cad_fi)
    |> Stream.map(&:iconv.convert("ISO-8859-1", "utf-8", &1))
    |> Stream.into(File.stream!(@cad_fi_store_file, encoding: :utf8))
    |> Stream.run()
  end

  def load_cad_FI_dataframe() do
    {:ok, cad_fi} = Explorer.DataFrame.from_csv(@cad_fi_store_file, @explorer_opts)
    cad_fi
  end

  def inf_diario_fi_dataframe(month, year) do
    year_month = year * 100 + month
    filename = "inf_diario_fi_#{year_month}.csv"
    {:ok, cad_fi} = Explorer.DataFrame.from_csv(filename, @explorer_opts)
    cad_fi
  end

  def download_diario_url(month, year) do
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
