import axios from "axios";
import mysql from 'mysql2/promise';
import dotenv from 'dotenv';

dotenv.config();

const PANDA_API_URL = "https://api-v2.pandavideo.com.br";
const PANDA_TOKEN = process.env.PANDA_TOKEN;

async function getDbConnection() {
  return mysql.createConnection({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASS,
    database: process.env.MYSQL_DB,
  });
}

async function findPandaVideoByTitle(folderId, title) {
  try {
    const res = await axios.get(`${PANDA_API_URL}/videos`, {
      headers: { Authorization: PANDA_TOKEN },
      params: { folder_id: folderId, title }
    });
    if (res.data && res.data.videos && res.data.videos.length > 0) {
      // Pode ajustar caso a API traga mais de um resultado
      return res.data.videos[0];
    }
    return null;
  } catch (e) {
    console.error(`[Panda] Erro ao buscar vídeo "${title}":`, e.message);
    return null;
  }
}

async function syncPandaVideoIds() {
  const conn = await getDbConnection();
  const [rows] = await conn.execute(
    "SELECT vimeo_video_id, panda_video_id, title FROM vimeo_panda_videos WHERE panda_video_id IS NULL"
  );

  if (rows.length === 0) {
    console.log("Nenhum vídeo pendente de sincronização.");
    await conn.end();
    return;
  }

  console.log(`[Sync] Encontrados ${rows.length} vídeos sem panda_video_id.`);

  for (const row of rows) {
    console.log(`[Sync] Buscando vídeo "${row.title}" no Panda...`);
    const pandaVideo = await findPandaVideoByTitle(row.folder_id, row.title);

    if (pandaVideo) {
      let formatedPandaId = formatPandaIdToUrl(pandaVideo.video_external_id)
      await conn.execute(
        "UPDATE vimeo_panda_videos SET panda_video_id = ? WHERE vimeo_video_id = ?",
        [formatedPandaId, row.vimeo_video_id]
      );
      console.log(`[Sync] Atualizado com panda_video_id: ${pandaVideo.id}`);
    } else {
      console.log(`[Sync] Vídeo "${row.title}" ainda não encontrado no Panda.`);
    }

    await new Promise(r => setTimeout(r, 500));
  }

  await conn.end();
  console.log("[Sync] Processo concluído.");
}

function formatPandaIdToUrl(videoId) {
  return `https://player-vz-4cab7bf9-47f.tv.pandavideo.com.br/embed/?v=${videoId}`;
}

syncPandaVideoIds();
