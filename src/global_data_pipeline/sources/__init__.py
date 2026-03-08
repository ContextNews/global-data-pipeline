from global_data_pipeline.sources.world_bank import WorldBankSource
from global_data_pipeline.sources.imf import IMFSource
from global_data_pipeline.sources.un_sdg import UNSDGSource

ALL_SOURCES = {
    "world_bank": WorldBankSource,
    "imf": IMFSource,
    "un_sdg": UNSDGSource,
}

__all__ = ["ALL_SOURCES", "WorldBankSource", "IMFSource", "UNSDGSource"]
