defmodule CvmExplorer do
  require Explorer.DataFrame, as: DF

  def getFIHistory() do
    Enum.to_list(1..3)
    |> Enum.map(fn month ->
      IO.inspect(month)
      CvmExplorer.DiarioFI.download(month, 2023)
      CvmExplorer.DiarioFI.dataframe(9, 2023)
    end)
    |> DF.concat_rows()
  end
end
