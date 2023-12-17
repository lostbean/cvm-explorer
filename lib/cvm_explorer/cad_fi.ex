defmodule CvmExplorer.CadFI do
  require Explorer.DataFrame, as: DF

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
    {"DT_INI_CLASSE", :date}
  ]
  @explorer_opts [
    delimiter: ";",
    dtypes: @col_mapping,
    nil_values: [""]
  ]

  def download() do
    HTTPStreamer.get(@cad_fi)
    |> Stream.map(&:iconv.convert("ISO-8859-1", "utf-8", &1))
    |> Stream.into(File.stream!(@cad_fi_store_file, encoding: :utf8))
    |> Stream.run()
  end

  def dataframe() do
    {:ok, cad_fi} = DF.from_csv(@cad_fi_store_file, @explorer_opts)
    cad_fi
  end
end
