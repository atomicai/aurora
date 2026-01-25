from typing import Any
from uuid import UUID

from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.outputs import LLMResult
from loguru import logger


class StreamingCallback(AsyncCallbackHandler):

    def __init__(self, typing_interval: int = 5, verbose: bool = True):
        self.accumulated_text = []
        self.tokens_received = 0
        self.typing_interval = typing_interval
        self.verbose = verbose

    async def on_llm_new_token(self, token: str, run_id, **props):
        self.accumulated_text.append(token)
        self.tokens_received += 1
        if self.tokens_received >= self.typing_interval:
            chunk = self.accumulated_text[-self.tokens_received :]
            if self.verbose:
                logger.info(
                    f"{self.__class__.__name__}.on_llm_new_token | run_id={run_id} | chunk={chunk}"
                )
            self.tokens_received = 0

    async def on_llm_end(
        self,
        response: LLMResult,
        *,
        run_id: UUID,
        parent_run_id: UUID | None = None,
        tags: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        self.response = response.generations[0][0].text
