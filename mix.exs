defmodule CvmExplorer.MixProject do
  use Mix.Project

  def project do
    [
      app: :cvm_explorer,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # Using main due a recent bug fix - https://github.com/elixir-explorer/explorer/issues/906
      {:explorer, "~> 0.8.3"},
      {:httpoison, "~> 2.2"},
      {:iconv, "~> 1.0"},
      {:zstream, "~> 0.6.4"}
    ]
  end
end
