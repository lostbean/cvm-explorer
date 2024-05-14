defmodule CvmExplorer.Utils.Dataframe do
  @moduledoc """
  Syncs the data from the external API to the database.
  """
  require Explorer.DataFrame, as: DF
  require Explorer.Query, as: DQ
  require Explorer.Series, as: DS

  def replace_infinity_and_nan(df) do
    num_cols =
      df
      |> DF.dtypes()
      |> Map.filter(fn {_, dtype} -> float_dtype?(dtype) end)
      |> Map.keys()

    Explorer.DataFrame.mutate_with(df, fn ldf ->
      Enum.map(num_cols, fn col ->
        {col, DQ.if(DS.is_finite(ldf[col]), do: ldf[col], else: nil)}
      end)
    end)
  end

  defp float_dtype?(dtype) do
    case dtype do
      {:f, _} -> true
      _ -> false
    end
  end
end
