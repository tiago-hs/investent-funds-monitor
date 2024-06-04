from investment_funds_monitor.bc_cdi_daily import BCDataCollector
from investment_funds_monitor.cvm_daily_inf_collector import CVMCollector


def main():
    cvm_save_path = "downloads"
    bacen_save_path = "bacen_cdi/daily_cdi.json"

    cvm_daily_data_url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/"
    cvm_hist_data_url = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/"
    bacen_cdi_url = (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json"
    )

    query_cvm_data = '//pre/a[contains(@href, "inf_diario_fi")]/@href'

    cvm_daily_datasets = CVMCollector(
        cvm_save_path, cvm_daily_data_url, query_cvm_data
    )
    cvm_hist_datasets = CVMCollector(
        cvm_save_path, cvm_hist_data_url, query_cvm_data 
    )
    bacen_cdi_dataset = BCDataCollector(bacen_cdi_url, bacen_save_path)

    cvm_daily_datasets.collect()
    cvm_hist_datasets.collect()
    bacen_cdi_dataset.collect()


if __name__ == "__main__":
    main()
