import axios from "axios";
import mysql from "mysql2/promise";
import dotenv from "dotenv";

dotenv.config();

const VIMEO_TOKEN = process.env.VIMEO_TOKEN;
const VIMEO_URL_BASE = process.env.VIMEO_URL_BASE;
const VIMEO_USER_ID = process.env.VIMEO_USER_ID;

const PANDA_API_URL = "https://api-v2.pandavideo.com.br";
const PANDA_TOKEN = process.env.PANDA_TOKEN;

// === Conexão com MySQL ===
async function getDbConnection() {
  return mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASS,
    database: process.env.MYSQL_DB,
  });
}

// === Clientes axios ===
const axiosVimeo = axios.create({
  baseURL: VIMEO_URL_BASE,
  headers: { Authorization: `Bearer ${VIMEO_TOKEN}` },
  timeout: 60000, // 60s para evitar socket hang up
});

const axiosPanda = axios.create({
  baseURL: PANDA_API_URL,
  headers: { Authorization: PANDA_TOKEN },
  timeout: 15000,
});

// === Helpers ===
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function vimeoVideoExists(pool, vimeoVideoUrl) {
  const [rows] = await pool.execute(
    "SELECT 1 FROM vimeo_panda_videos WHERE vimeo_video_id = ? LIMIT 1",
    [vimeoVideoUrl]
  );
  return rows.length > 0;
}

function formatVimeoUrl(path) {
  const match = path.match(/^\/videos\/(\d+)$/);
  if (!match) return null;
  return `https://player.vimeo.com/video/${match[1]}`;
}

function formatPandaIdToUrl(videoId) {
  return `https://player-vz-4cab7bf9-47f.tv.pandavideo.com.br/embed/?v=${videoId}`;
}

// === Request Vimeo com retry para 429 e conexões falhadas ===
async function axiosVimeoRetry(url, retries = 5) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await axiosVimeo.get(url);
    } catch (err) {
      if (err.response?.status === 429) {
        const retryAfter = parseInt(err.response.headers['retry-after'] || '5', 10);
        const waitTime = retryAfter * 1000 * (attempt + 1);
        console.warn(`[RATE LIMIT] Aguardando ${waitTime / 1000}s...`);
        await sleep(waitTime);
      } else if (err.code === 'ECONNRESET' || err.code === 'ETIMEDOUT') {
        const waitTime = 2000 * (attempt + 1);
        console.warn(`[CONN RESET] Tentando novamente em ${waitTime / 1000}s...`);
        await sleep(waitTime);
      } else {
        throw err;
      }
    }
  }
  throw new Error(`Falha ao acessar Vimeo após ${retries} tentativas: ${url}`);
}

// === Panda helpers ===
async function findPandaFolder(name, parentFolderId = null) {
  const res = await axiosPanda.get("/folders");
  const found = res.data?.folders?.find(
    f =>
      f.name === name &&
      ((f.parent_folder_id === parentFolderId) ||
        (f.parent_folder_id == null && parentFolderId == null))
  );
  return found ? found.id : null;
}

async function findPandaVideoByTitle(folderId, title) {
  const res = await axiosPanda.get("/videos", { params: { folder_id: folderId, title } });
  return res.data?.videos?.[0] || null;
}

// === Processa vídeos ===
async function processVideosInFolder(vimeoFolder, pandaFolderId, pool) {
  const videosUri = vimeoFolder.folder?.metadata?.connections?.videos?.uri;
  if (!videosUri) return;

  let url = videosUri + "?per_page=50";
  do {
    const res = await axiosVimeoRetry(url);
    const data = res.data;
    if (!data?.data) break;

    for (const video of data.data) {
      const title = video.name || "sem título";
      const vimeoVideoUrl = formatVimeoUrl(video.uri);
      if (!vimeoVideoUrl) continue;

      const exists = await vimeoVideoExists(pool, vimeoVideoUrl);
      if (exists) {
        console.log(`[SKIP] Já existe no DB: ${title}`);
        continue;
      }

      const pandaVideo = await findPandaVideoByTitle(pandaFolderId, title);
      if (pandaVideo) {
        const pandaVideoUrl = formatPandaIdToUrl(pandaVideo.video_external_id);
        await pool.execute(
          `INSERT INTO vimeo_panda_videos (vimeo_video_id, panda_video_id, title)
           VALUES (?, ?, ?)
           ON DUPLICATE KEY UPDATE
             panda_video_id = VALUES(panda_video_id),
             title = VALUES(title),
             updated_at = CURRENT_TIMESTAMP`,
          [vimeoVideoUrl, pandaVideoUrl, title]
        );
        console.log(`[MAP] ${title} -> ${pandaVideoUrl}`);
      } else {
        console.warn(`[MISS] Panda não encontrou: ${title}`);
      }

      await sleep(300); // pausa entre vídeos
    }

    url = data.paging?.next || null;
  } while (url);
}

// === Processa pastas recursivamente ===
async function processFolder(vimeoFolderUri, pandaParentId, pool) {
  let url = vimeoFolderUri + "?per_page=50";
  do {
    const res = await axiosVimeoRetry(url);
    const data = res.data;
    if (!data?.data) break;

    for (const folder of data.data) {
      const folderName = folder.folder?.name || "sem nome";
      const vimeoUri = folder.folder?.uri;
      if (!vimeoUri) continue;

      const pandaFolderId = await findPandaFolder(folderName, pandaParentId);
      if (!pandaFolderId) {
        console.warn(`[Skip] Pasta não encontrada no Panda: ${folderName}`);
        continue;
      }

      await processVideosInFolder(folder, pandaFolderId, pool);

      const subfoldersUri = folder.folder?.metadata?.connections?.items?.uri;
      if (subfoldersUri) {
        await processFolder(subfoldersUri, pandaFolderId, pool);
      }

      await sleep(500); // pausa entre pastas
    }

    url = data.paging?.next || null;
  } while (url);
}

// === Execução principal ===
async function run() {
  const pool = await getDbConnection();
  const rootUri = `/users/${VIMEO_USER_ID}/folders/root?direction=asc&exclude_personal_team_folder=true&exclude_shared_videos=false&no_padding=true&sort=alphabetical&responsive=true`;

  await processFolder(rootUri, null, pool);

  await pool.end();
  console.log("Processo concluído!");
}

run().catch(err => console.error(err));
