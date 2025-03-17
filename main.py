import asyncio
import collections
import datetime
import hashlib
import io
import json
import os
import shutil
import tempfile
import typing
import urllib
import urllib.parse
from collections.abc import AsyncGenerator
from pathlib import Path

import attrs
import httpx
import podgen
from bs4 import BeautifulSoup

client = httpx.AsyncClient(verify=False)


shows = {"Byte into IT"}


class B2Client:
    def __init__(self, key_id: str, key: str):
        self._key_id = key_id
        self._key = key
        self._auth_client = httpx.AsyncClient()
        self._token = None
        self._token_expire_at = None
        self._api_url = None
        self._client = None
        self._file_caches = collections.defaultdict[str, set](set)

    async def _ensure_token(self):
        now = datetime.datetime.now()
        if self._token_expire_at is not None and self._token_expire_at < now:
            return
        resp = await self._auth_client.get(
            "https://api.backblazeb2.com/b2api/v1/b2_authorize_account",
            auth=(self._key_id, self._key),
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["authorizationToken"]
        self._token_expire_at = now + datetime.timedelta(hours=6)
        self._api_url = data["apiUrl"]
        self._client = httpx.AsyncClient(
            base_url=self._api_url, headers={"Authorization": self._token}
        )

    async def _fill_cache(self, bucket_id: str) -> None:
        await self._ensure_token()
        cache = self._file_caches[bucket_id]
        cache.clear()
        next_name = None
        while True:
            params = {"bucketId": bucket_id, "maxFileCount": "10000"}
            if next_name is not None:
                params["startFileName"] = next_name
            resp = await self._client.get(
                "/b2api/v3/b2_list_file_names",
                params=params,
            )
            resp.raise_for_status()
            data = resp.json()
            for f in data["files"]:
                cache.add(f["fileName"])
            next_name = data["nextFileName"]
            if not next_name:
                break

    async def upload(self, bucket_id: str, key: str, file: typing.BinaryIO) -> None:
        await self._ensure_token()
        resp = await self._client.get(
            "/b2api/v3/b2_get_upload_url", params={"bucketId": bucket_id}
        )
        resp.raise_for_status()
        data = resp.json()
        upload_url = data["uploadUrl"]
        upload_token = data["authorizationToken"]

        sha = hashlib.file_digest(file, "sha1").hexdigest()
        file.seek(0, 0)

        length = 0
        while True:
            buf = file.read(1024 * 1024)
            if not buf:
                break
            length += len(buf)
        file.seek(0, 0)

        async def _content() -> AsyncGenerator[bytes]:
            while True:
                buf = file.read(1024 * 1024)
                if not buf:
                    break
                yield buf

        resp = await self._client.post(
            upload_url,
            content=_content(),
            headers={
                "Authorization": upload_token,
                "X-Bz-File-Name": urllib.parse.quote(key),
                "Content-Type": "b2/x-auto",
                "Content-Length": str(length),
                "X-Bz-Content-Sha1": sha,
            },
        )
        resp.raise_for_status()

    async def has_file(self, bucket_id: str, key: str) -> bool:
        await self._ensure_token()
        if not self._file_caches[bucket_id]:
            await self._fill_cache(bucket_id)
        return key in self._file_caches[bucket_id]


b2_client = B2Client(os.environ["B2_KEY_ID"], os.environ["B2_KEY"])


@attrs.define
class Episode:
    show: str
    image_url: str
    title: str
    subtitle: str
    player_item_id: int
    timestamp: str

    @property
    def parsed_timestamp(self) -> datetime.datetime:
        return datetime.datetime(
            int(self.timestamp[0:4]),
            int(self.timestamp[4:6]),
            int(self.timestamp[6:8]),
            int(self.timestamp[8:10]),
            int(self.timestamp[10:12]),
            int(self.timestamp[12:14]),
            tzinfo=datetime.UTC,
        )


async def parse_episodes_page(base: str, n: int) -> AsyncGenerator[Episode]:
    resp = await client.get(base, params={"page": str(n)})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.read(), "html5lib")
    for elm in soup.find_all("div", class_="card"):
        data_elm = elm.find(attrs={"data-view-playable": True})
        if data_elm is None:
            continue
        data = json.loads(data_elm.attrs["data-view-playable"])["items"][0]
        yield Episode(
            show=data["data"]["title"][: -len(data["data"]["subtitle"]) - 3],
            image_url=data["data"]["image"]["path"],
            title=data["data"]["title"],
            subtitle=data["data"]["subtitle"],
            player_item_id=data["player_item_id"],
            timestamp=data["data"]["timestamp"],
        )


async def parse_all_episode_pages(
    base: str = "https://www.rrr.org.au/on-demand/episodes",
) -> AsyncGenerator[Episode]:
    for i in range(500):
        found_any = False
        async for ep in parse_episodes_page(base, i + 1):
            found_any = True
            yield ep
        if not found_any:
            break


async def download_chunk(
    sem: asyncio.Semaphore, chunk_url: str, out_path: Path
) -> None:
    async with sem:
        with out_path.open("wb") as outf:
            async with client.stream("GET", chunk_url) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes():
                    outf.write(chunk)


async def download_episode(ep: Episode) -> Path:
    resp = await client.get(
        "https://ondemand.rrr.org.au/getclip?",
        params={"bw": "h", "l": "0", "m": "r", "p": "1", "s": ep.timestamp},
    )
    assert resp.status_code == 307
    playlist_url = resp.headers["Location"]
    resp = await client.get(playlist_url)
    resp.raise_for_status()
    playlist_data = resp.text.splitlines()
    chunklist_name = playlist_data[3]
    assert chunklist_name.endswith(".m3u8")
    chunklist_url = playlist_url.rsplit("/", 1)[0] + "/" + chunklist_name
    resp = await client.get(chunklist_url)
    resp.raise_for_status()
    chunks = [line for line in resp.text.splitlines() if not line.startswith("#")]
    assert all(c.endswith(".aac") for c in chunks)
    out_folder = Path(tempfile.mkdtemp("rrr_rss"))
    sem = asyncio.BoundedSemaphore(10)
    tasks = []
    for c in chunks:
        chunk_url = playlist_url.rsplit("/", 1)[0] + "/" + c
        tasks.append(
            asyncio.create_task(download_chunk(sem, chunk_url, out_folder / c))
        )
    await asyncio.wait(tasks)
    input_txt_path = out_folder / "input.txt"
    with input_txt_path.open("w") as list_f:
        for c in chunks:
            list_f.write("file '")
            list_f.write(c)
            list_f.write("'\n")
    out_f = tempfile.NamedTemporaryFile(suffix=".aac", delete=False)
    out_path = Path(out_f.name)
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-f",
        "concat",
        "-i",
        str(input_txt_path),
        "-c",
        "copy",
        str(out_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    (stdout_bytes, _) = await proc.communicate()
    shutil.rmtree(str(out_folder))
    if proc.returncode != 0:
        print(stdout_bytes.decode())
        assert proc.returncode == 0
    return out_path


async def upload_episode(ep: Episode, path: Path) -> None:
    with path.open("rb") as in_f:
        await b2_client.upload(
            os.environ.get("BUCKET_ID"), f"{ep.player_item_id}.aac", in_f
        )


async def check_episode(ep: Episode) -> None:
    has = await b2_client.has_file(
        os.environ.get("BUCKET_ID"), f"{ep.player_item_id}.aac"
    )
    if has:
        return
    print(f"Syncing {ep.title}")
    p = await download_episode(ep)
    try:
        await upload_episode(ep, p)
    finally:
        p.unlink()


async def generate_podcast(eps: list[Episode]) -> str:
    p = podgen.Podcast()
    p.name = eps[0].show
    p.description = eps[0].show
    p.website = "https://www.rrr.org.au/"
    p.explicit = False
    p.image = eps[0].image_url
    for ep in eps:
        p.add_episode(
            podgen.Episode(
                title=ep.title,
                subtitle=ep.subtitle,
                summary=ep.show,
                long_summary=ep.show,
                media=podgen.Media.create_from_server_response(
                    url=f"https://rrr-rss.com/{ep.player_item_id}.aac"
                ),
                publication_date=ep.parsed_timestamp,
            )
        )
    return str(p)


async def upload_podcasts(eps: list[Episode]) -> None:
    eps_by_show = collections.defaultdict(list)
    for ep in eps:
        eps_by_show[ep.show].append(ep)
    for show, show_eps in eps_by_show.items():
        pod = await generate_podcast(show_eps)
        pod_f = io.BytesIO(str(pod).encode())
        await b2_client.upload(os.environ.get("BUCKET_ID"), f"{show}.xml", pod_f)


async def upload_index(eps: list[Episode]) -> None:
    shows = set(ep.show for ep in eps)
    page_f = io.StringIO()
    page_f.write(
        """
<!doctype html>
<html lang="en">
	<head>
		<meta charset="utf-8">
		<meta http-equiv="X-UA-Compatible" content="IE=edge">
		<title>RRR RSS</title>
		<meta name="viewport" content="width=device-width, initial-scale=1">
	</head>
	<body>
"""
    )
    for show in sorted(shows):
        page_f.write('<p><a href="/')
        page_f.write(urllib.parse.quote_plus(show))
        page_f.write('.xml">')
        page_f.write(show)
        page_f.write("</a></p>")
    page_f.write("</body></html>")
    await b2_client.upload(
        os.environ.get("BUCKET_ID"),
        "index.html",
        io.BytesIO(page_f.getvalue().encode()),
    )


async def main() -> None:
    eps = []
    async for ep in parse_all_episode_pages(
        "https://www.rrr.org.au/explore/programs/byte-into-it/episodes/page"
    ):
        await check_episode(ep)
        eps.append(ep)
    await upload_podcasts(eps)
    await upload_index(eps)


if __name__ == "__main__":
    asyncio.run(main())
