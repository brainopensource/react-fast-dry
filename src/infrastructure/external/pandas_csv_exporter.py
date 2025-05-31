from src.domain.ports import CsvExporterPort


class PandasCsvExporter(CsvExporterPort):
    def export(self, data: list, file_path: str) -> None:
        import pandas as pd
        df = pd.DataFrame([item.model_dump() for item in data])
        df.to_csv(file_path, index=False)
