#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2026 tmwgsicp
# Licensed under the GNU Affero General Public License v3.0
# See LICENSE file in the project root for full license text.
# SPDX-License-Identifier: AGPL-3.0-only
"""
RSS 订阅路由
订阅管理 + RSS XML 输出
"""

import csv
import io
import os
import re
import time
import logging
from datetime import datetime, timezone
from html import escape as html_escape
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

from utils import rss_store
from utils.rss_poller import rss_poller, POLL_INTERVAL
from utils.image_proxy import proxy_image_url
from utils.rss_streaming import (
    generate_single_rss_stream, 
    generate_historical_rss_stream,
    generate_aggregated_rss_stream,
    generate_category_rss_stream
)

logger = logging.getLogger(__name__)

DEFAULT_EXPORT_TIMEZONE = "Asia/Shanghai"
UNKNOWN_ACCOUNT_NAME = "未知公众号"
TITLE_PREFIX_RE = re.compile(r"^\[([^\]]+)\]\s*(.*)$")


def get_base_url(request: Request) -> str:
    """
    获取服务的基础 URL，优先使用环境变量 SITE_URL，
    支持反向代理（检测 X-Forwarded-Proto 和 X-Forwarded-Host）
    """
    # 优先使用配置的 SITE_URL
    site_url = os.getenv("SITE_URL", "").strip()
    if site_url:
        return site_url.rstrip("/")
    
    # 检测反向代理头部
    proto = request.headers.get("X-Forwarded-Proto", "http")
    host = request.headers.get("X-Forwarded-Host") or request.headers.get("Host", "localhost:5000")
    
    return f"{proto}://{host}"


def _get_zoneinfo(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        raise HTTPException(status_code=422, detail=f"无效时区: {timezone_name}")


def _validate_date_string(date_value: str) -> str:
    try:
        datetime.strptime(date_value, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=422, detail="date 必须是 YYYY-MM-DD 格式")
    return date_value


def _today_date_string(timezone_name: str) -> str:
    return datetime.now(_get_zoneinfo(timezone_name)).strftime("%Y-%m-%d")


def _target_date_string(date_value: Optional[str], timezone_name: str) -> str:
    if date_value:
        return _validate_date_string(date_value)
    return _today_date_string(timezone_name)


def _article_local_date(publish_time: int, timezone_name: str) -> str:
    return (
        datetime.fromtimestamp(publish_time, tz=timezone.utc)
        .astimezone(_get_zoneinfo(timezone_name))
        .strftime("%Y-%m-%d")
    )


def _split_account_and_title(title: str, fallback_account: str = "") -> tuple[str, str]:
    match = TITLE_PREFIX_RE.match(title or "")
    if match:
        account = match.group(1).strip() or UNKNOWN_ACCOUNT_NAME
        clean_title = match.group(2).strip() or title
        return account, clean_title
    return fallback_account or UNKNOWN_ACCOUNT_NAME, title or ""


def _group_category_articles_by_account(
    category_id: int,
    date_value: Optional[str],
    timezone_name: str,
    limit: int,
) -> Dict[str, List[dict]]:
    category = rss_store.get_category(category_id)
    if not category:
        raise HTTPException(status_code=404, detail="分类不存在")

    target_date = _validate_date_string(date_value) if date_value else None
    subs = rss_store.get_subscriptions_by_category(category_id)
    nickname_map = {s["fakeid"]: s.get("nickname") or s["fakeid"] for s in subs}
    articles = rss_store.get_articles_by_category(category_id, limit=limit) if subs else []

    grouped: Dict[str, List[dict]] = {}
    for article in articles:
        publish_time = article.get("publish_time")
        if target_date:
            if not publish_time:
                continue
            if _article_local_date(publish_time, timezone_name) != target_date:
                continue

        fakeid = article.get("fakeid", "")
        fallback_account = nickname_map.get(fakeid, fakeid)
        account, title = _split_account_and_title(article.get("title", ""), fallback_account)
        grouped.setdefault(account, []).append({
            "title": title,
            "link": article.get("link", ""),
        })

    return grouped

router = APIRouter()

# RSS 配置常量 - 动态限制策略
# [2026-05-06 优化] 根据场景设置不同默认值和上限，降低内存占用
#
# 核心区别：
# - 常规 RSS（单个/聚合/分类）：动态滚动更新，限制较小，节省内存
# - 历史 RSS：静态归档内容，一次性加载，上限较高，避免文章遗漏

RSS_SINGLE_DEFAULT = 30      # 单个公众号：默认 30，覆盖 6-15 天
RSS_SINGLE_MAX = 50          # 单个公众号：最大 50

RSS_AGGREGATED_DEFAULT = 4500    # 聚合 RSS：默认最大值，由窗口函数内部逻辑控制
RSS_AGGREGATED_MAX = 4500        # 聚合 RSS：最大 4500

RSS_CATEGORY_DEFAULT = 4500  # 分类 RSS：默认最大值，由窗口函数内部逻辑控制
RSS_CATEGORY_MAX = 4500      # 分类 RSS：最大 4500

RSS_HISTORICAL_DEFAULT = 500 # 历史 RSS：默认 500（付费内容，一次性加载）
RSS_HISTORICAL_MAX = 5000    # 历史 RSS：最大 5000（支持大量历史文章，避免遗漏）


# ── Pydantic models ──────────────────────────────────────

class SubscribeRequest(BaseModel):
    fakeid: str = Field(..., description="公众号 FakeID")
    nickname: str = Field("", description="公众号名称")
    alias: str = Field("", description="公众号微信号")
    head_img: str = Field("", description="头像 URL")


class SubscribeResponse(BaseModel):
    success: bool
    message: str = ""


class SubscriptionItem(BaseModel):
    fakeid: str
    nickname: str
    alias: str
    head_img: str
    created_at: int
    last_poll: int
    article_count: int = 0
    rss_url: str = ""


class SubscriptionListResponse(BaseModel):
    success: bool
    data: list = []


class PollerStatusResponse(BaseModel):
    success: bool
    data: dict = {}


# ── 订阅管理 ─────────────────────────────────────────────

@router.post("/rss/subscribe", response_model=SubscribeResponse, summary="添加 RSS 订阅")
async def subscribe(req: SubscribeRequest, request: Request):
    """
    添加一个公众号到 RSS 订阅列表。

    添加后，后台轮询器会定时拉取该公众号的最新文章。

    **请求体参数：**
    - **fakeid** (必填): 公众号 FakeID，通过搜索接口获取
    - **nickname** (可选): 公众号名称
    - **alias** (可选): 公众号微信号
    - **head_img** (可选): 公众号头像 URL
    """
    added = rss_store.add_subscription(
        fakeid=req.fakeid,
        nickname=req.nickname,
        alias=req.alias,
        head_img=req.head_img,
    )
    if added:
        logger.info("RSS subscription added: %s (%s)", req.nickname, req.fakeid[:8])
        return SubscribeResponse(success=True, message="订阅成功")
    return SubscribeResponse(success=True, message="已订阅，无需重复添加")


@router.delete("/rss/subscribe/{fakeid}", response_model=SubscribeResponse,
               summary="取消 RSS 订阅")
async def unsubscribe(fakeid: str):
    """
    取消订阅一个公众号，同时删除该公众号的缓存文章。

    **路径参数：**
    - **fakeid**: 公众号 FakeID
    """
    removed = rss_store.remove_subscription(fakeid)
    if removed:
        logger.info("RSS subscription removed: %s", fakeid[:8])
        return SubscribeResponse(success=True, message="已取消订阅")
    return SubscribeResponse(success=False, message="未找到该订阅")


@router.get("/rss/subscriptions", response_model=SubscriptionListResponse,
            summary="获取订阅列表")
async def get_subscriptions(request: Request):
    """
    获取当前所有 RSS 订阅的公众号列表。

    返回每个订阅的基本信息、缓存文章数和 RSS 地址。
    """
    subs = rss_store.list_subscriptions()
    base_url = get_base_url(request)

    items = []
    for s in subs:
        # 将头像 URL 转换为代理链接
        head_img = proxy_image_url(s.get("head_img", ""), base_url)
        fakeid = s['fakeid']
        # 统计历史文章数量
        historical_count = rss_store.count_historical_articles(fakeid)
        items.append({
            **s,
            "head_img": head_img,
            "rss_url": f"{base_url}/api/rss/{fakeid}",
            "historical_rss_url": f"{base_url}/api/rss/{fakeid}/history" if historical_count > 0 else "",
            "historical_count": historical_count,
        })

    return SubscriptionListResponse(success=True, data=items)


@router.post("/rss/poll", response_model=PollerStatusResponse,
             summary="手动触发轮询")
async def trigger_poll():
    """
    手动触发一次轮询，立即拉取所有订阅公众号的最新文章。

    通常用于首次订阅后立即获取文章，无需等待下一个轮询周期。
    """
    if not rss_poller.is_running:
        return PollerStatusResponse(
            success=False,
            data={"message": "轮询器未启动"}
        )
    try:
        await rss_poller.poll_now()
        return PollerStatusResponse(
            success=True,
            data={"message": "轮询完成"}
        )
    except Exception as e:
        return PollerStatusResponse(
            success=False,
            data={"message": f"轮询出错: {str(e)}"}
        )


@router.get("/rss/status", response_model=PollerStatusResponse,
            summary="轮询器状态")
async def poller_status():
    """
    获取 RSS 轮询器运行状态。
    """
    subs = rss_store.list_subscriptions()
    return PollerStatusResponse(
        success=True,
        data={
            "running": rss_poller.is_running,
            "poll_interval": POLL_INTERVAL,
            "subscription_count": len(subs),
        },
    )


# ── 聚合 RSS ─────────────────────────────────────────────

@router.get("/rss/all", summary="聚合 RSS 订阅源",
            response_class=Response)
async def get_aggregated_rss_feed(
    request: Request,
    limit: int = Query(RSS_AGGREGATED_DEFAULT, ge=1, le=RSS_AGGREGATED_MAX, description="文章数量上限"),
):
    """
    获取所有订阅公众号的聚合 RSS 2.0 订阅源。

    将此地址添加到 RSS 阅读器，即可在一个订阅源中查看所有公众号文章。
    订阅增减后自动生效，无需更换链接。
    """
    subs = rss_store.list_subscriptions()
    nickname_map = {s["fakeid"]: s.get("nickname") or s["fakeid"] for s in subs}

    articles = rss_store.get_all_articles(limit=limit) if subs else []

    base_url = get_base_url(request)
    
    # [2026-05-08 优化] 使用流式生成降低内存占用
    return StreamingResponse(
        generate_aggregated_rss_stream(articles, nickname_map, base_url),
        media_type="application/rss+xml; charset=utf-8",
        headers={"Cache-Control": "public, max-age=600"},
    )


@router.get("/rss/category/{category_id}", summary="分类 RSS 订阅源",
            response_class=Response)
async def get_category_rss_feed(
    category_id: int,
    request: Request,
    limit: int = Query(RSS_CATEGORY_DEFAULT, ge=1, le=RSS_CATEGORY_MAX, description="文章数量上限"),
):
    """
    获取指定分类下所有订阅公众号的聚合 RSS 2.0 订阅源。

    将此地址添加到 RSS 阅读器，即可查看该分类内所有公众号文章。
    分类内订阅增减后自动生效，无需更换链接。
    """
    category = rss_store.get_category(category_id)
    if not category:
        raise HTTPException(status_code=404, detail="分类不存在")

    subs = rss_store.get_subscriptions_by_category(category_id)
    nickname_map = {s["fakeid"]: s.get("nickname") or s["fakeid"] for s in subs}
    articles = rss_store.get_articles_by_category(category_id, limit=limit) if subs else []

    base_url = get_base_url(request)

    return StreamingResponse(
        generate_category_rss_stream(category, articles, nickname_map, base_url),
        media_type="application/rss+xml; charset=utf-8",
        headers={"Cache-Control": "public, max-age=600"},
    )


@router.get("/rss/category/{category_id}/articles", summary="导出分类文章")
async def export_category_articles(
    category_id: int,
    date: Optional[str] = Query(None, regex=r"^\d{4}-\d{2}-\d{2}$", description="可选目标日期，格式 YYYY-MM-DD；不传则不按日期过滤"),
    timezone_name: str = Query(DEFAULT_EXPORT_TIMEZONE, alias="timezone", description="日期过滤时区"),
    limit: int = Query(RSS_CATEGORY_DEFAULT, ge=1, le=RSS_CATEGORY_MAX, description="文章数量上限"),
) -> Dict[str, List[dict]]:
    """
    导出指定分类文章，按公众号名称分组。

    - 不传 date: 返回分类下文章（受 limit 限制）
    - 传 date=YYYY-MM-DD: 只返回该日期更新的文章

    返回格式:
    {
      "公众号名称": [
        {"title": "文章标题", "link": "文章链接"}
      ]
    }
    """
    return _group_category_articles_by_account(category_id, date, timezone_name, limit)


@router.get("/rss/category/{category_id}/today", summary="导出分类今日文章")
async def export_category_today_articles(
    category_id: int,
    timezone_name: str = Query(DEFAULT_EXPORT_TIMEZONE, alias="timezone", description="日期过滤时区"),
    limit: int = Query(RSS_CATEGORY_DEFAULT, ge=1, le=RSS_CATEGORY_MAX, description="文章数量上限"),
) -> Dict[str, List[dict]]:
    """
    导出指定分类今天更新的文章，按公众号名称分组。
    """
    today = _today_date_string(timezone_name)
    return _group_category_articles_by_account(category_id, today, timezone_name, limit)


# ── 导出 ─────────────────────────────────────────────────

@router.get("/rss/export", summary="导出订阅列表")
async def export_subscriptions(
    request: Request,
    format: str = Query("csv", regex="^(csv|opml)$", description="导出格式: csv 或 opml"),
    scope: str = Query("subscriptions", regex="^(subscriptions|categories)$", description="导出范围: subscriptions 或 categories"),
):
    """
    导出 RSS 列表。

    - **scope=subscriptions**: 导出单个公众号 RSS 列表
    - **scope=categories**: 导出分类聚合 RSS 列表
    - **csv**: 表格格式
    - **opml**: 标准 OPML 格式，可直接导入 RSS 阅读器
    """
    base_url = get_base_url(request)

    if scope == "categories":
        categories = rss_store.list_categories()
        if format == "opml":
            return _build_category_opml_response(categories, base_url)
        return _build_category_csv_response(categories, base_url)

    subs = rss_store.list_subscriptions()
    if format == "opml":
        return _build_opml_response(subs, base_url)
    return _build_csv_response(subs, base_url)


def _build_csv_response(subs: list, base_url: str) -> Response:
    buf = io.StringIO()
    buf.write('\ufeff')
    writer = csv.writer(buf)
    writer.writerow(["Name", "FakeID", "RSS URL", "Articles", "Subscribed At"])
    for s in subs:
        rss_url = f"{base_url}/api/rss/{s['fakeid']}"
        sub_date = datetime.fromtimestamp(
            s.get("created_at", 0), tz=timezone.utc
        ).strftime("%Y-%m-%d")
        writer.writerow([
            s.get("nickname") or s["fakeid"],
            s["fakeid"],
            rss_url,
            s.get("article_count", 0),
            sub_date,
        ])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="wechat_rss_subscriptions.csv"'},
    )


def _build_category_csv_response(categories: list, base_url: str) -> Response:
    buf = io.StringIO()
    buf.write('\ufeff')
    writer = csv.writer(buf)
    writer.writerow(["Category", "RSS URL", "Subscriptions", "Created At"])
    for c in categories:
        rss_url = f"{base_url}/api/rss/category/{c['id']}"
        created_at = c.get("created_at", 0)
        created_date = datetime.fromtimestamp(
            created_at, tz=timezone.utc
        ).strftime("%Y-%m-%d") if created_at else ""
        writer.writerow([
            c.get("name") or c["id"],
            rss_url,
            c.get("subscription_count", 0),
            created_date,
        ])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="wechat_rss_categories.csv"'},
    )


def _build_opml_response(subs: list, base_url: str) -> Response:
    opml = ET.Element("opml", version="2.0")
    head = ET.SubElement(opml, "head")
    ET.SubElement(head, "title").text = "WeChat RSS Subscriptions"
    ET.SubElement(head, "dateCreated").text = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )

    body = ET.SubElement(opml, "body")
    group = ET.SubElement(body, "outline", text="WeChat RSS", title="WeChat RSS")

    for s in subs:
        name = s.get("nickname") or s["fakeid"]
        rss_url = f"{base_url}/api/rss/{s['fakeid']}"
        ET.SubElement(group, "outline", **{
            "type": "rss",
            "text": name,
            "title": name,
            "xmlUrl": rss_url,
            "htmlUrl": "https://mp.weixin.qq.com",
            "description": f"{name} - WeChat RSS",
        })

    xml_str = ET.tostring(opml, encoding="unicode", xml_declaration=False)
    content = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str

    return Response(
        content=content,
        media_type="application/xml; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="wechat_rss_subscriptions.opml"'},
    )


def _build_category_opml_response(categories: list, base_url: str) -> Response:
    opml = ET.Element("opml", version="2.0")
    head = ET.SubElement(opml, "head")
    ET.SubElement(head, "title").text = "WeChat RSS Categories"
    ET.SubElement(head, "dateCreated").text = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )

    body = ET.SubElement(opml, "body")
    group = ET.SubElement(body, "outline", text="WeChat RSS Categories", title="WeChat RSS Categories")

    for c in categories:
        name = c.get("name") or str(c["id"])
        rss_url = f"{base_url}/api/rss/category/{c['id']}"
        description = c.get("description") or f"{name} - WeChat RSS Category"
        ET.SubElement(group, "outline", **{
            "type": "rss",
            "text": name,
            "title": name,
            "xmlUrl": rss_url,
            "htmlUrl": base_url,
            "description": description,
        })

    xml_str = ET.tostring(opml, encoding="unicode", xml_declaration=False)
    content = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str

    return Response(
        content=content,
        media_type="application/xml; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="wechat_rss_categories.opml"'},
    )


@router.get("/rss/{fakeid}/history", summary="公众号历史文章 RSS 订阅源",
            response_class=Response)
async def get_historical_rss_feed(
    fakeid: str,
    request: Request,
    page: int = Query(1, ge=1, description="页码"),
    limit: int = Query(RSS_HISTORICAL_DEFAULT, ge=1, le=RSS_HISTORICAL_MAX, description="每页文章数量"),
):
    """
    获取指定公众号通过历史文章功能拉取的 RSS 2.0 归档源。
    """
    sub = rss_store.get_subscription(fakeid)
    if not sub:
        raise HTTPException(status_code=404, detail="订阅不存在")

    total_count = rss_store.count_historical_articles(fakeid)
    total_pages = max(1, (total_count + limit - 1) // limit)
    if page > total_pages:
        raise HTTPException(status_code=404, detail="页码超出范围")

    offset = (page - 1) * limit
    articles = rss_store.get_historical_articles(fakeid, limit=limit, offset=offset)
    base_url = get_base_url(request)

    return StreamingResponse(
        generate_historical_rss_stream(fakeid, sub, articles, base_url, page, total_pages, total_count),
        media_type="application/rss+xml; charset=utf-8",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@router.get("/rss/{fakeid}", summary="公众号 RSS 订阅源",
            response_class=Response)
async def get_single_rss_feed(
    fakeid: str,
    request: Request,
    limit: int = Query(RSS_SINGLE_DEFAULT, ge=1, le=RSS_SINGLE_MAX, description="文章数量上限"),
):
    """
    获取指定公众号的 RSS 2.0 订阅源。
    """
    sub = rss_store.get_subscription(fakeid)
    if not sub:
        raise HTTPException(status_code=404, detail="订阅不存在")

    articles = rss_store.get_regular_articles(fakeid, limit=limit)
    base_url = get_base_url(request)

    return StreamingResponse(
        generate_single_rss_stream(fakeid, sub, articles, base_url),
        media_type="application/rss+xml; charset=utf-8",
        headers={"Cache-Control": "public, max-age=600"},
    )


# ── RSS XML 输出 ──────────────────────────────────────────

def _rfc822(ts: int) -> str:
    """Unix 时间戳 → RFC 822 日期字符串"""
    if not ts:
        return ""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%a, %d %b %Y %H:%M:%S +0000")
