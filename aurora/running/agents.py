import abc
import json
from typing import Any, AsyncIterator, Dict, Iterator

import json_repair
from langchain_core.callbacks import AsyncCallbackManager
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from loguru import logger

from aurora.configuring.prime import AgentsConfig, AgentsConfigPipeline, Config


class IPromptRunner(abc.ABC):
    def __init__(self, system_prompt: str, llm: ChatOpenAI = None):
        self.system_prompt = system_prompt
        self.llm = llm  # includes callbacks to propagate to telegram chatter etc.

    @abc.abstractmethod
    def _prepare(self, user_text: str, **props) -> str:
        pass

    def prompt(self, system_prompt: str = None, **props) -> list[dict]:
        obj = self._prepare(**props)
        system_prompt = system_prompt or self.system_prompt
        return [
            dict(role="system", content=self.system_prompt),
            dict(role="user", content=obj),
        ]

    async def arun(self, messages: list[BaseMessage] | None = None, **kwargs) -> str:
        prompt = self._prepare(**kwargs)
        messages = (
            [SystemMessage(self.system_prompt)]
            + messages
            + [HumanMessage(content=prompt)]
            if messages is not None
            else [
                SystemMessage(content=self.system_prompt),
                HumanMessage(content=prompt),
            ]
        )
        resp = await self.llm.agenerate(messages=[messages])
        return resp.generations[0][0].text

    async def astream(self, **kwargs) -> AsyncIterator[str]:
        prompt = self._prepare(**kwargs)
        async for chunk in self.llm.astream(
            messages=[
                [
                    SystemMessage(content=self.system_prompt),
                    HumanMessage(content=prompt),
                ]
            ]
        ):
            yield chunk

    @abc.abstractmethod
    def finalize(self, user_text: str, raw_response: str, **props) -> str:
        pass


class AIBasePrompt(IPromptRunner):
    _system_prompt = f"""
    You are a highly capable language assistant with remarkable skillset on the following:
    - History and mechanics of computer games.
    - Well-versed in many films.
    - Skilled at providing user support and guidance for complex systems (e.g. user portals, 
      databases, or other technical domains).
    - Scientific facts and general historical facts
    """  # noqa

    def __init__(self, system_prompt: str = None, llm: ChatOpenAI = None):
        super().__init__(
            system_prompt=(
                system_prompt.strip()
                if system_prompt is not None
                else self._system_prompt.strip()
            ),
            llm=llm,
        )

    def _prepare(self, user_text: str, **props) -> str:
        return f"{user_text}"

    def finalize(
        self, content: str, raw_response: str, as_json_string: bool = False, **props
    ) -> str:
        try:
            repaired = json_repair.loads(raw_response)
        except Exception:
            repaired = {"error": raw_response}

        if as_json_string:
            return json.dumps(repaired, ensure_ascii=False)
        return json.dumps(repaired, ensure_ascii=False, indent=2)


# class GenericDialogueAgentRunner(IPromptRunner):

#     config_key_name: str = "default"

#     def __init__(self, system_prompt: str = None):
#         pass


# class UniverseRestrictedAgentRunner(IPromptRunner):

#     config_key_name: str = "universe"

#     def __init__(self, fpath):
#         pass
