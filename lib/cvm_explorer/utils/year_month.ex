defmodule CvmExplorer.Utils.YearMonth do
  @spec add(integer() | {integer(), integer()}, integer()) :: number() | {integer(), integer()}
  def add({year, month}, x) do
    ym = 12 * year + (month - 1) + x
    y = div(ym, 12)
    m = ym - 12 * y
    {y, m + 1}
  end

  def add(year_month, x) do
    year_month
    |> decode()
    |> add(x)
    |> encode()
  end

  @spec diff(integer() | {integer(), integer()}, integer() | {integer(), integer()}) :: integer()
  def diff({yearA, monthA}, {yearB, monthB}) do
    ymA = 12 * yearA + (monthA - 1)
    ymB = 12 * yearB + (monthB - 1)
    ymA - ymB
  end

  def diff(year_monthA, year_monthB) do
    diff(decode(year_monthA), decode(year_monthB))
  end

  @spec decode(integer()) :: {integer(), integer()}
  def decode(year_month) do
    year = div(year_month, 100)
    month = year_month - year * 100
    {year, month}
  end

  @spec encode({number(), number()}) :: number()
  def encode({year, month}) do
    year * 100 + month
  end
end
