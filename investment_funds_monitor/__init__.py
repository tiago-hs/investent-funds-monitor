from investment_funds_monitor.cvm_daily_inf_collector import CVMCollector


def main():
    save_path = "downloads"
    query_daily_data = '//pre/a[contains(@href, "inf_diario_fi")]/@href'
    query_data_hist = '//pre/a[contains(@href, "inf_diario_fi")]/@href'
    daily_data_url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
    hist_data_url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/"

    cvm_daily_datasets = CVMCollector(save_path, daily_data_url, query_daily_data)
    cvm_hist_datasets = CVMCollector(save_path, hist_data_url, query_data_hist)

    cvm_daily_datasets.collect()
    cvm_hist_datasets.collect()


if __name__ == "__main__":
    main()
