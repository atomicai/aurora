import contextlib
import os
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

from loguru import logger
from rethinkdb import r

from aurora.etc.schema import (
    ItemEventInputSchema,
    ItemEventSchema,
    ItemEventTypeEnum,
    ItemFeedSchema,
    ItemInputSchema,
    ItemStatsSchema,
    PersonalNameInputSchema,
    PersonalNameSchema,
    TargetEventInputSchema,
    TargetEventSchema,
    TargetEventTypeEnum,
    TargetInputSchema,
    TargetSchema,
    TargetStatsSchema,
    UserInputSchema,
    UserSchema,
)
from aurora.tools import normalize_str, normalize_str_list_keep_duplicates


class RethinkDocStore:
    def __init__(
        self, host: str | None = None, port: int | None = None, db: str | None = None
    ):
        self.host = host or os.getenv("RETHINKDB_HOST")
        self.port = port or int(os.getenv("RETHINKDB_PORT", "28015"))
        self.db = (
            normalize_str(db) or normalize_str(os.getenv("RETHINKDB_DB")) or "aurora"
        )

        self.conn = None
        r.set_loop_type("asyncio")

        self._uuid_namespace = uuid.UUID("c48c6e56-3b59-4ed2-bd2f-143d8cb0e6a4")

    def _stable_uuid(self, *parts: object) -> str:
        normalized = "|".join(str(p).strip() for p in parts if p is not None)
        return str(uuid.uuid5(self._uuid_namespace, normalized))

    async def _assert_all_ids_exist(self, table, ids: list[str], *, kind: str) -> None:
        ids = [str(v).strip() for v in (ids or []) if v]
        if not ids:
            return

        cursor = await table.get_all(*ids).pluck("id").run(self.conn)
        found: set[str] = set()
        if isinstance(cursor, list):
            found = {
                d["id"]
                for d in cursor
                if isinstance(d, dict) and isinstance(d.get("id"), str) and d.get("id")
            }
        else:
            async for d in cursor:
                if isinstance(d, dict) and d.get("id"):
                    found.add(d["id"])

        missing = [i for i in ids if i not in found]
        if missing:
            raise ValueError(f"Missing {kind} ids: {missing}")

    async def _assert_ids_belong_to_item(
        self, table, ids: list[str], *, item_id: str, kind: str
    ) -> None:
        ids = [str(v).strip() for v in (ids or []) if v]
        if not ids:
            return

        cursor = await table.get_all(*ids).pluck("id", "item_id").run(self.conn)
        mismatched: list[str] = []
        if isinstance(cursor, list):
            for doc in cursor:
                if not isinstance(doc, dict):
                    continue
                if doc.get("item_id") != item_id:
                    mismatched.append(str(doc.get("id")))
        else:
            async for doc in cursor:
                if not isinstance(doc, dict):
                    continue
                if doc.get("item_id") != item_id:
                    mismatched.append(str(doc.get("id")))

        if mismatched:
            raise ValueError(f"{kind} do not belong to item_id={item_id}: {mismatched}")

    async def upsert_target(self, *, item_id: str, data: dict) -> dict:
        """Upsert a Target document and return it.

        Id is deterministic by default: uuid5('target' + item_id + content).
        """

        payload = TargetInputSchema.model_validate(data)
        targets_table = r.db(self.db).table("targets")

        normalized_item_id = normalize_str(payload.item_id) or normalize_str(item_id)
        if not normalized_item_id:
            raise ValueError("item_id is required for target")

        target_id = (payload.id or "").strip() or self._stable_uuid(
            "target", normalized_item_id, payload.content
        )

        existing = await targets_table.get(target_id).run(self.conn)
        created_at_value = (
            existing.get("created_at") if isinstance(existing, dict) else None
        )

        record = {
            "id": target_id,
            "item_id": normalized_item_id,
            "content": payload.content,
            "explanation": payload.explanation,
            "level": payload.level,
            "score": payload.score,
            "reasons": payload.reasons,
            "created_at": created_at_value or r.now(),
            "updated_at": r.now(),
        }

        if existing:
            await targets_table.get(target_id).update(record).run(self.conn)
        else:
            await targets_table.insert(record).run(self.conn)

        stored = await targets_table.get(target_id).run(self.conn)
        return TargetSchema.model_validate(stored).model_dump()

    async def upsert_personal_name(self, *, item_id: str, data: dict) -> dict:
        """Upsert a PersonalName document and return it.

        Id is deterministic by default: uuid5('personal_name' + item_id + name).
        """

        payload = PersonalNameInputSchema.model_validate(data)
        personal_names_table = r.db(self.db).table("personal_names")

        normalized_item_id = normalize_str(payload.item_id) or normalize_str(item_id)
        if not normalized_item_id:
            raise ValueError("item_id is required for personal name")

        pn_id = (payload.id or "").strip() or self._stable_uuid(
            "personal_name", normalized_item_id, payload.name
        )

        existing = await personal_names_table.get(pn_id).run(self.conn)
        created_at_value = (
            existing.get("created_at") if isinstance(existing, dict) else None
        )

        record = {
            "id": pn_id,
            "item_id": normalized_item_id,
            "name": payload.name,
            "url": payload.url,
            "created_at": created_at_value or r.now(),
            "updated_at": r.now(),
        }

        if existing:
            await personal_names_table.get(pn_id).update(record).run(self.conn)
        else:
            await personal_names_table.insert(record).run(self.conn)

        stored = await personal_names_table.get(pn_id).run(self.conn)
        return PersonalNameSchema.model_validate(stored).model_dump()

    async def connect(self):
        """Установить соединение с RethinkDB"""
        self.conn = await r.connect(host=self.host, port=self.port, db=self.db)
        logger.info(f"Connected to RethinkDB at {self.host}:{self.port}, DB: {self.db}")

    async def close(self):
        """Закрыть соединение с RethinkDB"""
        if self.conn:
            await self.create_back_log(
                log_data="RethinkDB connection closed.",
                log_owner="RethinkDocStore.close",
            )
            await self.conn.close(noreply_wait=False)
            logger.info("RethinkDB connection closed.")

            self.conn = None

    async def on_startup_prepare_structure(self):
        """
        Инициализация БД и необходимых таблиц:
        - users
        - items
        - targets
        - personal_names
        - threads
        - messages
        - kv
        - pipeline
        - backlogs
        - item_events
        - target_events
        - item_stats
        - target_stats
        """
        async with await r.connect(host=self.host, port=self.port) as conn:
            logger.info("Initializing database and tables in RethinkDB.")

            db_list = await r.db_list().run(conn)
            if self.db not in db_list:
                await r.db_create(self.db).run(conn)

            tables = await r.db(self.db).table_list().run(conn)
            required_tables = [
                "users",
                "items",
                "targets",
                "personal_names",
                "threads",
                "messages",
                "kv",
                "pipeline",
                "backlogs",
                "item_events",
                "target_events",
                "item_stats",
                "target_stats",
            ]
            for table in required_tables:
                if table not in tables:
                    await r.db(self.db).table_create(table).run(conn)
                await r.db(self.db).wait().run(conn)

            await self._ensure_indexes(conn)

            logger.info("Database and tables are ready.")

    async def _ensure_indexes(self, conn) -> None:
        index_specs: dict[str, list[str]] = {
            "items": ["created_at"],
            "targets": ["item_id"],
            "personal_names": ["item_id"],
            "messages": ["thread_id", "created_at"],
            "threads": ["user_id", "created_at"],
            "item_events": ["user_id", "item_id", "event_type", "created_at"],
            "target_events": ["user_id", "target_id", "event_type", "created_at"],
            "item_stats": ["views", "likes", "saves"],
            "target_stats": ["views", "clicks", "saves"],
        }

        for table_name, indexes in index_specs.items():
            table = r.db(self.db).table(table_name)
            existing = await table.index_list().run(conn)
            for index_name in indexes:
                if index_name not in existing:
                    await table.index_create(index_name).run(conn)
            await table.index_wait().run(conn)

    async def create_item_sample(self, data: dict):
        """Create or update an Item with Targets/PersonalNames.

        Canonical id-based only:
        - target_ids/personal_names_ids + rows in targets/personal_names
        """

        payload = ItemInputSchema.model_validate(data)

        items_table = r.db(self.db).table("items")
        targets_table = r.db(self.db).table("targets")
        personal_names_table = r.db(self.db).table("personal_names")

        universe = (payload.universe or "").strip()
        title = (payload.title or "").strip()
        card = payload.card
        short_card_description = payload.short_card_description

        item_id = (payload.id or "").strip() or self._stable_uuid(
            "item", universe, title, card
        )

        provided_target_ids = payload.target_ids or []
        provided_personal_names_ids = payload.personal_names_ids or []

        # Strict canonical format: item references must point to existing docs.
        await self._assert_all_ids_exist(
            targets_table, provided_target_ids, kind="targets"
        )
        await self._assert_all_ids_exist(
            personal_names_table, provided_personal_names_ids, kind="personal_names"
        )

        await self._assert_ids_belong_to_item(
            targets_table, provided_target_ids, item_id=item_id, kind="targets"
        )
        await self._assert_ids_belong_to_item(
            personal_names_table,
            provided_personal_names_ids,
            item_id=item_id,
            kind="personal_names",
        )

        existing_item = await items_table.get(item_id).run(self.conn)
        created_at_value = (
            existing_item.get("created_at") if isinstance(existing_item, dict) else None
        )

        item_record = {
            "id": item_id,
            "universe": universe,
            "title": title,
            "card": card,
            "short_card_description": short_card_description,
            "score": payload.score,
            "target_ids": provided_target_ids,
            "personal_names_ids": provided_personal_names_ids,
            "created_at": created_at_value or r.now(),
            "updated_at": r.now(),
        }

        if existing_item:
            await items_table.get(item_id).update(item_record).run(self.conn)
        else:
            await items_table.insert(item_record).run(self.conn)

        await self.create_back_log(
            log_data=(
                "Item upserted (canonical ids only): "
                f"item_id={item_id}, target_ids={len(provided_target_ids)}, personal_names_ids={len(provided_personal_names_ids)}"
            ),
            log_owner="RethinkDocStore.create_item_sample",
        )

        expanded = await self._expand_item(
            item_record,
            targets_table=targets_table,
            personal_names_table=personal_names_table,
        )
        if expanded is None:
            expanded = {
                **item_record,
                "targets": [],
                "personal_names_docs": [],
            }
        return ItemFeedSchema.model_validate(expanded).model_dump()

    async def ingest_item_from_pipeline(self, data: dict) -> dict:
        """Ingest denormalized pipeline payload into canonical tables.

        Input is expected to ALWAYS be in the pipeline format. Any deviation:
        - logs error
        - raises exception

        Expected keys (minimal):
        - id: str
        - target: list[str]
        - card: str
        - short_card_description: str | None
        - personal_names: list[str] | None
        - explanations: list[{text:str, explanation:str}]
        - overall: str
        - targets: list[{text:str, level:str|None, score:float|None, reasons:list[str]}]
        """

        try:

            def fail(message: str) -> None:
                raise AssertionError(message)

            if not isinstance(data, dict):
                fail("pipeline payload must be a dict")

            raw_item_id = data.get("id")
            if not isinstance(raw_item_id, str) or not raw_item_id.strip():
                fail("missing/invalid id")
            assert isinstance(raw_item_id, str)
            item_id = raw_item_id.strip()

            raw_target_list = data.get("target")
            if not isinstance(raw_target_list, list):
                fail("missing/invalid target list")
            assert isinstance(raw_target_list, list)
            target_list = normalize_str_list_keep_duplicates(raw_target_list)
            if not target_list:
                fail("target must be list[str]")

            raw_personal_names = data.get("personal_names")
            if raw_personal_names is None:
                personal_names = []
            else:
                if not isinstance(raw_personal_names, list):
                    fail("personal_names must be list[str]|None")
                personal_names = normalize_str_list_keep_duplicates(raw_personal_names)

            card = data.get("card")
            if not isinstance(card, str) or not card.strip():
                fail("missing/invalid card")

            short_card_description = data.get("short_card_description")
            if short_card_description is not None and not isinstance(
                short_card_description, str
            ):
                fail("short_card_description must be str|None")

            universe = normalize_str(data.get("universe"))
            title = normalize_str(data.get("title"))

            raw_overall = data.get("overall")
            if not isinstance(raw_overall, str) or not raw_overall.strip():
                fail("missing/invalid overall")
            assert isinstance(raw_overall, str)
            overall = raw_overall.strip()

            raw_explanations = data.get("explanations")
            if not isinstance(raw_explanations, list) or not all(
                isinstance(e, dict) for e in raw_explanations
            ):
                fail("missing/invalid explanations")
            assert isinstance(raw_explanations, list)

            raw_targets = data.get("targets")
            if not isinstance(raw_targets, list) or not all(
                isinstance(t, dict) for t in raw_targets
            ):
                fail("missing/invalid targets")
            assert isinstance(raw_targets, list)

            # Duplicates are forbidden
            if len(set(target_list)) != len(target_list):
                fail(f"duplicate target entries: {target_list}")
            if len(set(personal_names)) != len(personal_names):
                fail(f"duplicate personal_names entries: {personal_names}")

            exp_texts: list[str] = []
            exp_map: dict[str, str] = {}
            for e in raw_explanations:
                text = e.get("text")
                explanation = e.get("explanation")
                if not isinstance(text, str) or not text.strip():
                    fail("explanations[].text must be str")
                if not isinstance(explanation, str) or not explanation.strip():
                    fail("explanations[].explanation must be str")

                text = text.strip()
                exp_texts.append(text)
                exp_map[text] = explanation

            if len(set(exp_texts)) != len(exp_texts):
                fail(f"duplicate explanations for texts: {exp_texts}")

            tgt_texts: list[str] = []
            targets_map: dict[str, dict] = {}
            for t in raw_targets:
                text = t.get("text")
                if not isinstance(text, str) or not text.strip():
                    fail("targets[].text must be str")
                text = text.strip()
                tgt_texts.append(text)
                targets_map[text] = t

            if len(set(tgt_texts)) != len(tgt_texts):
                fail(f"duplicate targets for texts: {tgt_texts}")

            # Tolerant mismatch handling (pipeline may miss difficulty/explanations)
            target_set = set(target_list)
            exp_set = set(exp_texts)
            tgt_set = set(tgt_texts)

            missing_explanations = target_set - exp_set
            if missing_explanations:
                logger.warning(
                    "Missing explanations for targets: "
                    f"{sorted(missing_explanations)}"
                )

            missing_targets = target_set - tgt_set
            if missing_targets:
                logger.warning(
                    "Missing difficulty targets for texts: "
                    f"{sorted(missing_targets)}"
                )

            extra_explanations = exp_set - target_set
            if extra_explanations:
                logger.warning(
                    "Extra explanations not in target list: "
                    f"{sorted(extra_explanations)}"
                )

            extra_targets = tgt_set - target_set
            if extra_targets:
                logger.warning(
                    "Extra difficulty targets not in target list: "
                    f"{sorted(extra_targets)}"
                )

            # Upsert docs first
            target_ids: list[str] = []
            for text in target_list:
                t = targets_map.get(text) or {}
                record = {
                    "item_id": item_id,
                    "content": text,
                    "explanation": exp_map.get(text),
                    "level": t.get("level"),
                    "score": t.get("score"),
                    "reasons": t.get("reasons") or [],
                }
                stored = await self.upsert_target(item_id=item_id, data=record)
                target_ids.append(stored["id"])

            personal_name_ids: list[str] = []
            for name in personal_names:
                stored = await self.upsert_personal_name(
                    item_id=item_id, data={"name": name, "item_id": item_id}
                )
                personal_name_ids.append(stored["id"])

            # Finally: create/update item strictly by ids
            return await self.create_item_sample(
                {
                    "id": item_id,
                    "universe": universe,
                    "title": title,
                    "card": card,
                    "short_card_description": short_card_description,
                    "score": overall,
                    "target_ids": target_ids,
                    "personal_names_ids": personal_name_ids,
                }
            )
        except Exception as e:
            with contextlib.suppress(Exception):
                payload_keys = (
                    list(data.keys()) if isinstance(data, dict) else str(type(data))
                )
                await self.create_back_log(
                    log_data=(
                        f"ingest_item_from_pipeline error: {e}; payload_keys={payload_keys}"
                    ),
                    log_owner="RethinkDocStore.ingest_item_from_pipeline",
                )
            raise

    def _validate_feed_item(self, item: dict) -> dict:
        for key in (
            "explanations",
            "difficulty",
            "overall_difficulty",
            "targets_difficulty",
            "image_url",
        ):
            item.pop(key, None)

        if item.get("targets"):
            item["targets"] = [
                TargetSchema.model_validate(t).model_dump() for t in item["targets"]
            ]
        if item.get("personal_names_docs"):
            item["personal_names_docs"] = [
                PersonalNameSchema.model_validate(p).model_dump()
                for p in item["personal_names_docs"]
            ]
        return ItemFeedSchema.model_validate(item).model_dump()

    async def _expand_item(
        self,
        item: dict,
        *,
        targets_table,
        personal_names_table,
    ) -> dict | None:
        item_id = item.get("id")
        if not item_id:
            return None

        t_list: list[dict] = []
        t_ids = item.get("target_ids") or []
        if t_ids:
            t_cursor = await targets_table.get_all(*t_ids).run(self.conn)
            if isinstance(t_cursor, list):
                t_list = t_cursor
            else:
                async for t in t_cursor:
                    t_list.append(t)

            t_order = {t_id: i for i, t_id in enumerate(t_ids)}
            t_list.sort(key=lambda t: t_order.get(t.get("id"), 10**9))

        pn_list: list[dict] = []
        pn_ids = item.get("personal_names_ids") or []
        if pn_ids:
            pn_cursor = await personal_names_table.get_all(*pn_ids).run(self.conn)
            if isinstance(pn_cursor, list):
                pn_list = pn_cursor
            else:
                async for pn in pn_cursor:
                    pn_list.append(pn)

            pn_order = {p_id: i for i, p_id in enumerate(pn_ids)}
            pn_list.sort(key=lambda p: pn_order.get(p.get("id"), 10**9))

        if t_list:
            item.setdefault(
                "target", [t.get("content") for t in t_list if t.get("content")]
            )
        else:
            item.setdefault("target", [])

        if pn_list:
            item.setdefault(
                "personal_names",
                [pn.get("name") for pn in pn_list if pn.get("name")],
            )
        else:
            item.setdefault("personal_names", [])

        expanded = {
            **item,
            "targets": t_list,
            "personal_names_docs": pn_list,
        }
        return self._validate_feed_item(expanded)

    async def fetch_item_by_id(self, item_id: str) -> dict | None:
        items_table = r.db(self.db).table("items")
        targets_table = r.db(self.db).table("targets")
        personal_names_table = r.db(self.db).table("personal_names")

        item = await items_table.get(item_id).run(self.conn)
        if not item:
            return None

        expanded = await self._expand_item(
            item,
            targets_table=targets_table,
            personal_names_table=personal_names_table,
        )
        return expanded

    async def fetch_target_by_id(self, target_id: str) -> dict | None:
        targets_table = r.db(self.db).table("targets")
        target = await targets_table.get(target_id).run(self.conn)
        if not target:
            return None
        return TargetSchema.model_validate(target).model_dump()

    async def iter_items_per_user(
        self,
        user_id: int,
        *,
        offset: int = 0,
        exclude_seen: bool = True,
        exclude_liked: bool = True,
        exclude_disliked: bool = True,
    ) -> AsyncIterator[dict]:
        """Async iterator over a user's feed.

        Intended usage:
            async for item in store.iter_items_per_user(user_id):
                ...
        """

        users_table = r.db(self.db).table("users")
        items_table = r.db(self.db).table("items")
        targets_table = r.db(self.db).table("targets")
        personal_names_table = r.db(self.db).table("personal_names")

        user = await users_table.get(user_id).run(self.conn)
        if not user:
            await self.create_back_log(
                log_data=f"User not found: user_id={user_id}",
                log_owner="RethinkDocStore.iter_items_per_user",
            )
            return

        offset = int(offset)
        excluded_item_ids: set[str] = set()
        if exclude_seen:
            excluded_item_ids.update(user.get("seen_item_ids") or [])
        if exclude_liked:
            excluded_item_ids.update(user.get("liked_item_ids") or [])
        if exclude_disliked:
            excluded_item_ids.update(user.get("disliked_item_ids") or [])

        cursor = await items_table.order_by(r.desc("created_at")).run(self.conn)
        skipped = 0
        yielded = 0

        try:
            if isinstance(cursor, list):
                for doc in cursor:
                    if doc.get("id") in excluded_item_ids:
                        continue
                    if skipped < offset:
                        skipped += 1
                        continue

                    expanded = await self._expand_item(
                        doc,
                        targets_table=targets_table,
                        personal_names_table=personal_names_table,
                    )
                    if expanded is None:
                        continue

                    yielded += 1
                    yield expanded
            else:
                async for doc in cursor:
                    if doc.get("id") in excluded_item_ids:
                        continue
                    if skipped < offset:
                        skipped += 1
                        continue

                    expanded = await self._expand_item(
                        doc,
                        targets_table=targets_table,
                        personal_names_table=personal_names_table,
                    )
                    if expanded is None:
                        continue

                    yielded += 1
                    yield expanded
        finally:
            await self.create_back_log(
                log_data=(
                    "Feed iter finished: "
                    f"user_id={user_id}, yielded={yielded}, offset={offset}, "
                    f"exclude_seen={exclude_seen}, exclude_liked={exclude_liked}, exclude_disliked={exclude_disliked}"
                ),
                log_owner="RethinkDocStore.iter_items_per_user",
            )

    async def fetch_items_per_user(
        self,
        user_id: int,
        offset: int = 0,
        *,
        top_k: int | None = None,
        exclude_seen: bool = True,
        exclude_liked: bool = True,
        exclude_disliked: bool = True,
    ):
        """Fetch feed items.

        - If `top_k` is provided: returns a list of items.
        - If not provided: returns an async iterator (use `async for`).
        """

        k = top_k
        iterator = self.iter_items_per_user(
            user_id,
            offset=offset,
            exclude_seen=exclude_seen,
            exclude_liked=exclude_liked,
            exclude_disliked=exclude_disliked,
        )

        if k is None:
            return iterator

        k = int(k)
        if k <= 0:
            return []

        results: list[dict] = []
        async for item in iterator:
            results.append(item)
            if len(results) >= k:
                break

        await self.create_back_log(
            log_data=f"Fetched {len(results)} items for user_id={user_id} (top_k={k}, offset={offset})",
            log_owner="RethinkDocStore.fetch_items_per_user",
        )
        return results

    async def _ensure_user_row(self, user_id: int | str) -> None:
        users_table = r.db(self.db).table("users")
        existing = await users_table.get(user_id).run(self.conn)
        if existing:
            return

        record = UserSchema(
            id=user_id,
            seen_item_ids=[],
            liked_item_ids=[],
            disliked_item_ids=[],
            saved_item_ids=[],
            skipped_item_ids=[],
            seen_target_ids=[],
            saved_target_ids=[],
            clicked_target_ids=[],
        ).model_dump()
        record["created_at"] = r.now()
        record["updated_at"] = r.now()
        await users_table.insert(record).run(self.conn)

    async def _increment_stats(
        self, table_name: str, stats_id: str, inc_fields: dict[str, int]
    ) -> None:
        table = r.db(self.db).table(table_name)
        existing = await table.get(stats_id).run(self.conn)

        if existing:
            update_doc = {
                key: r.row.get_field(key).default(0).add(int(value))
                for key, value in inc_fields.items()
            }
            update_doc["updated_at"] = r.now()
            await table.get(stats_id).update(update_doc).run(self.conn)
            return

        record = {
            "id": stats_id,
            **{key: int(value) for key, value in inc_fields.items()},
            "created_at": r.now(),
            "updated_at": r.now(),
        }
        await table.insert(record).run(self.conn)

    async def record_item_event(
        self,
        *,
        user_id: int | str,
        item_id: str,
        event_type: ItemEventTypeEnum | str,
        meta: dict[str, Any] | None = None,
    ) -> dict:
        payload = ItemEventInputSchema.model_validate(
            {
                "user_id": user_id,
                "item_id": item_id,
                "event_type": event_type,
                "meta": meta or {},
            }
        )

        await self._ensure_user_row(payload.user_id)

        event_id = str(uuid.uuid4())
        record = {
            "id": event_id,
            "user_id": payload.user_id,
            "item_id": payload.item_id,
            "event_type": str(payload.event_type),
            "meta": payload.meta,
            "created_at": r.now(),
        }

        item_events = r.db(self.db).table("item_events")
        await item_events.insert(record).run(self.conn)

        await self._update_user_item_lists(payload.user_id, payload.item_id, payload)
        await self._increment_stats(
            "item_stats", payload.item_id, self._item_stats_delta(payload.event_type)
        )

        stored = await item_events.get(event_id).run(self.conn)
        return ItemEventSchema.model_validate(stored).model_dump()

    async def record_target_event(
        self,
        *,
        user_id: int | str,
        target_id: str,
        event_type: TargetEventTypeEnum | str,
        meta: dict[str, Any] | None = None,
    ) -> dict:
        payload = TargetEventInputSchema.model_validate(
            {
                "user_id": user_id,
                "target_id": target_id,
                "event_type": event_type,
                "meta": meta or {},
            }
        )

        await self._ensure_user_row(payload.user_id)

        event_id = str(uuid.uuid4())
        record = {
            "id": event_id,
            "user_id": payload.user_id,
            "target_id": payload.target_id,
            "event_type": str(payload.event_type),
            "meta": payload.meta,
            "created_at": r.now(),
        }

        target_events = r.db(self.db).table("target_events")
        await target_events.insert(record).run(self.conn)

        await self._update_user_target_lists(
            payload.user_id, payload.target_id, payload
        )
        await self._increment_stats(
            "target_stats",
            payload.target_id,
            self._target_stats_delta(payload.event_type),
        )

        stored = await target_events.get(event_id).run(self.conn)
        return TargetEventSchema.model_validate(stored).model_dump()

    def _item_stats_delta(self, event_type: ItemEventTypeEnum | str) -> dict[str, int]:
        event = str(event_type)
        if event == ItemEventTypeEnum.view.value:
            return {"views": 1}
        if event == ItemEventTypeEnum.like.value:
            return {"likes": 1, "views": 1}
        if event == ItemEventTypeEnum.dislike.value:
            return {"dislikes": 1, "views": 1}
        if event == ItemEventTypeEnum.save.value:
            return {"saves": 1, "views": 1}
        if event == ItemEventTypeEnum.skip.value:
            return {"skips": 1, "views": 1}
        if event == ItemEventTypeEnum.ask.value:
            return {"asks": 1}
        return {"views": 1}

    async def count_item_events(
        self, *, user_id: int | str, item_id: str, event_type: str
    ) -> int:
        item_events = r.db(self.db).table("item_events")
        count = (
            await item_events.filter(
                {
                    "user_id": user_id,
                    "item_id": item_id,
                    "event_type": event_type,
                }
            )
            .count()
            .run(self.conn)
        )
        return int(count)

    def _target_stats_delta(
        self, event_type: TargetEventTypeEnum | str
    ) -> dict[str, int]:
        event = str(event_type)
        if event == TargetEventTypeEnum.view.value:
            return {"views": 1}
        if event == TargetEventTypeEnum.save.value:
            return {"saves": 1, "views": 1}
        if event == TargetEventTypeEnum.click.value:
            return {"clicks": 1, "views": 1}
        return {"views": 1}

    async def _update_user_item_lists(
        self, user_id: int | str, item_id: str, payload: ItemEventInputSchema
    ) -> None:
        users_table = r.db(self.db).table("users")

        updates: dict[str, Any] = {
            "seen_item_ids": r.row["seen_item_ids"].default([]).set_insert(item_id)
        }

        event = str(payload.event_type)
        if event == ItemEventTypeEnum.like.value:
            updates["liked_item_ids"] = (
                r.row["liked_item_ids"].default([]).set_insert(item_id)
            )
        elif event == ItemEventTypeEnum.dislike.value:
            updates["disliked_item_ids"] = (
                r.row["disliked_item_ids"].default([]).set_insert(item_id)
            )
        elif event == ItemEventTypeEnum.save.value:
            updates["saved_item_ids"] = (
                r.row["saved_item_ids"].default([]).set_insert(item_id)
            )
        elif event == ItemEventTypeEnum.skip.value:
            updates["skipped_item_ids"] = (
                r.row["skipped_item_ids"].default([]).set_insert(item_id)
            )

        updates["updated_at"] = r.now()
        await users_table.get(user_id).update(updates).run(self.conn)

    async def _update_user_target_lists(
        self, user_id: int | str, target_id: str, payload: TargetEventInputSchema
    ) -> None:
        users_table = r.db(self.db).table("users")

        updates: dict[str, Any] = {
            "seen_target_ids": r.row["seen_target_ids"]
            .default([])
            .set_insert(target_id)
        }

        event = str(payload.event_type)
        if event == TargetEventTypeEnum.save.value:
            updates["saved_target_ids"] = (
                r.row["saved_target_ids"].default([]).set_insert(target_id)
            )
        elif event == TargetEventTypeEnum.click.value:
            updates["clicked_target_ids"] = (
                r.row["clicked_target_ids"].default([]).set_insert(target_id)
            )

        updates["updated_at"] = r.now()
        await users_table.get(user_id).update(updates).run(self.conn)

    async def fetch_top_items(
        self, *, metric: str = "views", limit: int = 50
    ) -> list[dict]:
        metric = metric.strip()
        if metric not in {"views", "likes", "dislikes", "saves", "skips"}:
            raise ValueError("Invalid item metric")

        stats_table = r.db(self.db).table("item_stats")
        cursor = await stats_table.order_by(r.desc(metric)).limit(limit).run(self.conn)

        if isinstance(cursor, list):
            return [ItemStatsSchema.model_validate(row).model_dump() for row in cursor]

        rows = []
        async for row in cursor:
            rows.append(ItemStatsSchema.model_validate(row).model_dump())
        return rows

    async def fetch_top_targets(
        self, *, metric: str = "views", limit: int = 50
    ) -> list[dict]:
        metric = metric.strip()
        if metric not in {"views", "saves", "clicks"}:
            raise ValueError("Invalid target metric")

        stats_table = r.db(self.db).table("target_stats")
        cursor = await stats_table.order_by(r.desc(metric)).limit(limit).run(self.conn)

        if isinstance(cursor, list):
            return [
                TargetStatsSchema.model_validate(row).model_dump() for row in cursor
            ]

        rows = []
        async for row in cursor:
            rows.append(TargetStatsSchema.model_validate(row).model_dump())
        return rows

    async def create_pipeline_log(
        self,
        message_id: str | None = None,
        log_data: str | None = None,
        log_owner: str | None = None,
        pipeline_version: str = "v1",
    ) -> None:
        pipeline_table = r.db(self.db).table("pipeline")

        log_id = str(uuid.uuid4())
        log_datatime = int(time.time() * 1000)

        log_record = {
            "log_id": log_id,
            "message_id": message_id,
            "log_data": log_data,
            "log_owner": log_owner,
            "log_datatime": log_datatime,
            "pipeline_version": pipeline_version,
        }

        await pipeline_table.insert(log_record).run(self.conn)
        logger.info(f"[PIPELINE] {log_owner}: {log_data}")

    async def create_back_log(
        self, log_data: str | None = None, log_owner: str | None = None
    ) -> None:
        backlogs_table = r.db(self.db).table("backlogs")
        log_id = str(uuid.uuid4())
        log_datatime = int(time.time() * 1000)

        log_record = {
            "log_id": log_id,
            "log_data": log_data,
            "log_owner": log_owner,
            "log_datatime": log_datatime,
        }

        await backlogs_table.insert(log_record).run(self.conn)
        logger.info(f"[BACKLOG] {log_owner}: {log_data}")

    async def upsert_user(self, user_data: dict, offset: int | None = None) -> dict:
        users_table = r.db(self.db).table("users")

        payload = UserInputSchema.model_validate(user_data)
        data = payload.model_dump()

        existing_user = await users_table.get(data["id"]).run(self.conn)

        if existing_user:
            updated_user = {**existing_user, **data}
            if offset is not None:
                updated_user["current_thread_offset"] = offset
            updated_user["updated_at"] = r.now()
            await users_table.get(data["id"]).update(updated_user).run(self.conn)

            logger.info(f"User {data['id']} data updated.")
            await self.create_back_log(
                log_data=f"User {data['id']} data updated.",
                log_owner="RethinkDocStore.upsert_user",
            )
            return UserSchema.model_validate(updated_user).model_dump()

        new_user = {
            "id": data["id"],
            **data,
            "current_thread_offset": offset or 0,
            "created_at": r.now(),
            "updated_at": r.now(),
        }
        await users_table.insert(new_user).run(self.conn)

        logger.info(f"User {data['id']} created.")
        await self.create_back_log(
            log_data=f"User {data['id']} created.",
            log_owner="RethinkDocStore.upsert_user",
        )
        return UserSchema.model_validate(new_user).model_dump()

    async def fetch_user_threads(
        self, user_id: int, limit: int = 10, offset: int = 0
    ) -> tuple[list[dict], int]:
        threads_table = r.db(self.db).table("threads")

        offset = int(offset)
        limit = int(limit)

        cursor = (
            await threads_table.filter({"user_id": user_id})
            .order_by(r.desc("created_at"))
            .slice(offset, offset + limit)
            .run(self.conn)
        )
        threads = cursor
        total_count = (
            await threads_table.filter({"user_id": user_id}).count().run(self.conn)
        )

        logger.info(
            f"Fetched {len(threads)} threads for user {user_id}, total threads={total_count}."
        )
        await self.create_back_log(
            log_data=f"Fetched {len(threads)} threads for user {user_id}, total threads={total_count}.",
            log_owner="RethinkDocStore.fetch_user_threads",
        )
        return threads, total_count

    async def create_or_update_thread(
        self,
        user_id: int,
        thread_id: int | None = None,
        title: str | None = None,
        set_active: bool = False,
    ) -> dict:
        threads_table = r.db(self.db).table("threads")
        users_table = r.db(self.db).table("users")

        if thread_id:
            thread = await threads_table.get(thread_id).run(self.conn)
            if not thread or thread["user_id"] != user_id:
                logger.info(
                    f"Thread {thread_id} not found or not owned by user {user_id}."
                )
                await self.create_back_log(
                    log_data=f"Thread {thread_id} not found or not owned by user {user_id}.",
                    log_owner="RethinkDocStore.create_or_update_thread",
                )
                raise ValueError("Thread not found or not owned by user.")
            if title:
                thread["title"] = title
            thread["updated_at"] = r.now()
            await threads_table.get(thread_id).update(thread).run(self.conn)
        else:
            thread = {"user_id": user_id, "title": title, "created_at": r.now()}
            result = await threads_table.insert(thread, return_changes=True).run(
                self.conn
            )
            thread = result["changes"][0]["new_val"]

        if set_active:
            await (
                users_table.get(user_id)
                .update({"active_thread_id": thread["id"]})
                .run(self.conn)
            )

        logger.info(
            f"Thread {thread['id']} created/updated for user {user_id}. set_active={set_active}."
        )
        await self.create_back_log(
            log_data=f"Thread {thread['id']} created/updated for user {user_id}. set_active={set_active}.",
            log_owner="RethinkDocStore.create_or_update_thread",
        )
        return thread

    async def fetch_active_thread(self, user_id: int) -> dict | None:
        users_table = r.db(self.db).table("users")
        threads_table = r.db(self.db).table("threads")

        user = await users_table.get(user_id).run(self.conn)
        if not user or not user.get("active_thread_id"):
            logger.info(f"No active thread found for user {user_id}.")
            await self.create_back_log(
                log_data=f"No active thread found for user {user_id}.",
                log_owner="RethinkDocStore.fetch_active_thread",
            )
            return None

        thread = await threads_table.get(user["active_thread_id"]).run(self.conn)

        logger.info(f"Fetched active thread {thread['id']} for user {user_id}.")
        await self.create_back_log(
            log_data=f"Fetched active thread {thread['id']} for user {user_id}.",
            log_owner="RethinkDocStore.fetch_active_thread",
        )
        return thread

    async def delete_thread(self, thread_id: int):
        threads_table = r.db(self.db).table("threads")
        messages_table = r.db(self.db).table("messages")
        users_table = r.db(self.db).table("users")

        thread = await threads_table.get(thread_id).run(self.conn)
        if not thread:
            raise ValueError("Thread not found.")

        await (
            users_table.filter({"active_thread_id": thread_id})
            .update({"active_thread_id": None})
            .run(self.conn)
        )

        await messages_table.filter({"thread_id": thread_id}).delete().run(self.conn)

        await threads_table.get(thread_id).delete().run(self.conn)

        logger.info(f"Thread {thread_id} and all related messages have been deleted.")
        await self.create_back_log(
            log_data=f"Thread {thread_id} and all related messages have been deleted.",
            log_owner="RethinkDocStore.delete_thread",
        )

    async def fetch_thread_by_id(self, thread_id: int) -> dict:
        threads_table = r.db(self.db).table("threads")
        thread = await threads_table.get(thread_id).run(self.conn)
        if not thread:
            raise ValueError("Thread not found.")

        logger.info(f"Thread {thread_id} retrieved.")
        await self.create_back_log(
            log_data=f"Thread {thread_id} retrieved.",
            log_owner="RethinkDocStore.fetch_thread_by_id",
        )
        return thread

    async def fetch_all_messages_by_thread_id(self, thread_id: int) -> list[dict]:
        messages_table = r.db(self.db).table("messages")
        cursor = (
            await messages_table.filter({"thread_id": thread_id})
            .order_by("created_at")
            .run(self.conn)
        )

        messages = []
        if isinstance(cursor, list):
            messages = cursor
        else:
            async for message in cursor:
                messages.append(message)

        logger.info(f"Fetched {len(messages)} messages for thread {thread_id}.")
        await self.create_back_log(
            log_data=f"Fetched {len(messages)} messages for thread {thread_id}.",
            log_owner="RethinkDocStore.fetch_all_messages_by_thread_id",
        )
        return messages

    async def add_message_to_thread(
        self,
        thread_id: int,
        text: str,
        message_type: str,
        rating: str | None = None,
        message_topic: str | None = None,
        is_relevant_towards_context: str | None = None,
        parent_id: str | None = None,
    ) -> dict:
        messages_table = r.db(self.db).table("messages")
        message = {
            "thread_id": thread_id,
            "text": text,
            "message_type": message_type,
            "rating": rating,
            "created_at": r.now(),
            "message_topic": message_topic,
            "parent_id": parent_id,
            "is_relevant_towards_context": is_relevant_towards_context,
        }
        result = await messages_table.insert(message, return_changes=True).run(
            self.conn
        )
        new_msg = result["changes"][0]["new_val"]

        logger.info(f"Message added to thread {thread_id}, type={message_type}")
        await self.create_back_log(
            log_data=f"Message added to thread {thread_id}, type={message_type}",
            log_owner="RethinkDocStore.add_message_to_thread",
        )
        return new_msg

    async def update_message(
        self,
        message_id: int,
        text: str | None = None,
        rating: str | None = None,
        message_topic: str | None = None,
        is_relevant_towards_context: str | None = None,
    ) -> dict:
        messages_table = r.db(self.db).table("messages")
        message = await messages_table.get(message_id).run(self.conn)
        if not message:
            raise ValueError("Message not found.")

        if text is not None:
            message["text"] = text
        if rating is not None:
            message["rating"] = rating
        if message_topic is not None:
            message["message_topic"] = message_topic
        if is_relevant_towards_context is not None:
            message["is_relevant_towards_context"] = is_relevant_towards_context

        message["updated_at"] = r.now()
        await messages_table.get(message_id).update(message).run(self.conn)

        logger.info(
            f"Message {message_id} updated (text={bool(text)}, rating={rating})"
        )
        await self.create_back_log(
            log_data=f"Message {message_id} updated (text={bool(text)}, rating={rating})",
            log_owner="RethinkDocStore.update_message",
        )
        return message

    async def get_value(self, key: str) -> str | None:
        kv_table = r.db(self.db).table("kv")
        kv_cursor = await kv_table.filter({"key": key}).run(self.conn)
        kv = None
        with contextlib.suppress(StopAsyncIteration):
            kv = await kv_cursor.next()

        value = kv["value"] if kv else None

        logger.info(f"Retrieved value for key={key}: {value}")
        await self.create_back_log(
            log_data=f"Retrieved value for key={key}: {value}",
            log_owner="RethinkDocStore.get_value",
        )
        return value

    async def set_value(self, key: str, value: str):
        kv_table = r.db(self.db).table("kv")
        existing_kv = await kv_table.get(key).run(self.conn)

        if existing_kv:
            await kv_table.get(key).update({"value": value}).run(self.conn)
        else:
            await kv_table.insert({"key": key, "value": value}).run(self.conn)

        logger.info(f"Set KV value: key={key}, value={value}")
        await self.create_back_log(
            log_data=f"Set KV value: key={key}, value={value}",
            log_owner="RethinkDocStore.set_value",
        )

    async def delete_value(self, key: str):
        kv_table = r.db(self.db).table("kv")
        await kv_table.get(key).delete().run(self.conn)

        logger.info(f"Deleted KV key: {key}")
        await self.create_back_log(
            log_data=f"Deleted KV key: {key}", log_owner="RethinkDocStore.delete_value"
        )

    async def fetch_keys(self) -> list[str]:
        kv_table = r.db(self.db).table("kv")
        cursor = await kv_table.pluck("key").run(self.conn)
        keys_list = await cursor.to_list()
        keys = [item["key"] for item in keys_list]

        logger.info(f"Fetched keys: {keys}")
        await self.create_back_log(
            log_data=f"Fetched keys: {keys}", log_owner="RethinkDocStore.fetch_keys"
        )
        return keys

    async def fetch_kv_pairs(self, keys: list[str]) -> dict[str, str | None]:
        kv_table = r.db(self.db).table("kv")
        cursor = await kv_table.filter(
            lambda row: r.expr(keys).contains(row["key"])
        ).run(self.conn)

        kv_list = []
        async for item in cursor:
            kv_list.append(item)

        result = {kv["key"]: kv.get("value") for kv in kv_list}

        logger.info(f"Retrieved KV pairs for keys: {keys}")
        await self.create_back_log(
            log_data=f"Retrieved KV pairs for keys: {keys}",
            log_owner="RethinkDocStore.fetch_kv_pairs",
        )
        return result

    async def bulk_set_if_not_exists(self, kv_dict: dict[str, str]) -> None:
        kv_table = r.db(self.db).table("kv")
        cursor = await kv_table.filter(
            lambda doc: r.expr(list(kv_dict.keys())).contains(doc["key"])
        ).run(self.conn)

        existing_keys = set()
        async for entry in cursor:
            existing_keys.add(entry["key"])

        new_kv_entries = [
            {"id": key, "key": key, "value": str(value)}
            for key, value in kv_dict.items()
            if key not in existing_keys
        ]

        if new_kv_entries:
            await kv_table.insert(new_kv_entries).run(self.conn)
            logger.info(
                f"Default bulk insert of KV without overwrite: {list(kv_dict.keys())}"
            )
            await self.create_back_log(
                log_data=f"Default bulk insert of KV without overwrite: {list(kv_dict.keys())}",
                log_owner="RethinkDocStore.bulk_set_if_not_exists",
            )
