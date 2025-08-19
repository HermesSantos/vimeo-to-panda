import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config();

import mysql from 'mysql2/promise';

// criar pool ou conexão única
const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASS,
  database: process.env.MYSQL_DB,
});

async function saveVideoMapping(vimeoVideoId, pandaVideoId, pandaWebsocketUrl, title) {
  const sql = `
    INSERT INTO vimeo_panda_videos (vimeo_video_id, panda_video_id, panda_websocket_url, title)
    VALUES (?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      panda_video_id = VALUES(panda_video_id),
      panda_websocket_url = VALUES(panda_websocket_url),
      title = VALUES(title),
      updated_at = CURRENT_TIMESTAMP
  `;
  await pool.execute(sql, [
    formatVimeoUrl(vimeoVideoId) ?? null,
    pandaVideoId ?? null,
    pandaWebsocketUrl ?? null,
    title ?? null
  ]);

  console.log(`[DB] Mapeamento salvo: ${vimeoVideoId} -> ${pandaVideoId}`);
}


const VIMEO_TOKEN = process.env.VIMEO_TOKEN;
const PANDA_TOKEN = process.env.PANDA_TOKEN;
const VIMEO_URL_BASE = process.env.VIMEO_URL_BASE;
const VIMEO_USER_ID = process.env.VIMEO_USER_ID;

const PANDA_API_BASE = 'https://api-v2.pandavideo.com.br';
const PANDA_UPLOAD_URL = 'https://import.pandavideo.com:9443/videos';

const axiosVimeo = axios.create({
  baseURL: VIMEO_URL_BASE,
  headers: { Authorization: `Bearer ${VIMEO_TOKEN}` },
  timeout: 15000,
});

const axiosPandaAPI = axios.create({
  baseURL: PANDA_API_BASE,
  headers: { Authorization: PANDA_TOKEN },
  timeout: 15000,
});

async function safeGet(client, url, config = {}, retry = 3) {
  for (let attempt = 1; attempt <= retry; attempt++) {
    try {
      const res = await client.get(url, config);
      return res.data;
    } catch (e) {
      const status = e.response?.status;
      console.error(`[GET][Attempt ${attempt}] Error fetching ${url}:`, e.message);

      if (status === 429) {
        // Tempo de espera vindo do header ou tempo crescente baseado na tentativa
        const wait =
          e.response.headers?.['retry-after']
            ? parseInt(e.response.headers['retry-after'], 10) * 1000
            : 2000 * attempt;

        console.warn(`⚠️ Rate limit (429) — aguardando ${wait / 1000}s antes de tentar novamente...`);
        await new Promise(r => setTimeout(r, wait));
        continue;
      }

      if (attempt === retry) throw e;
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

async function safePost(client, url, data = {}, config = {}, retry = 3) {
  for (let attempt = 1; attempt <= retry; attempt++) {
    try {
      const res = await client.post(url, data, config);
      return res.data;
    } catch (e) {
      const status = e.response?.status;
      console.error(`[POST][Attempt ${attempt}] Error posting to ${url}:`, e.message);

      if (status === 429) {
        const wait =
          e.response.headers?.['retry-after']
            ? parseInt(e.response.headers['retry-after'], 10) * 1000
            : 2000 * attempt;

        console.warn(`⚠️ Rate limit (429) — aguardando ${wait / 1000}s antes de tentar novamente...`);
        await new Promise(r => setTimeout(r, wait));
        continue;
      }

      if (attempt === retry) throw e;
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

/**
 * Busca pasta no Panda pelo nome e parent_folder_id
 */
async function findPandaFolder(name, parentFolderId = null) {
  try {
    const data = await safeGet(axiosPandaAPI, '/folders');

    if (!data || !Array.isArray(data.folders)) return null;

    const found = data.folders.find(f =>
      f.name === name &&
      ((f.parent_folder_id === parentFolderId) ||
       (f.parent_folder_id == null && parentFolderId == null))
    );

    if (found) {
      console.log(`[Panda] Pasta encontrada: "${name}" (ID: ${found.id})`);
      return found.id;
    }

    return null;
  } catch (e) {
    console.error(`[Panda] Erro ao buscar pasta "${name}":`, e.message);
    return null;
  }
}

/**
 * Cria pasta no Panda
 */
async function createPandaFolder(name, parentFolderId = null) {
  try {
    const payload = { name };
    if (parentFolderId) {
      payload.parent_folder_id = parentFolderId;
    }
    const data = await safePost(axiosPandaAPI, '/folders', payload);

    if (data && data.id) {
      console.log(`[Panda] Pasta criada: "${name}" (ID: ${data.id})`);
      return data.id;
    }
    console.error(`[Panda] Falha ao criar pasta "${name}", resposta inesperada`, data);
    return null;
  } catch (e) {
    console.error(`[Panda] Erro ao criar pasta "${name}":`, e.message);
    return null;
  }
}

/**
 * Upload do vídeo para Panda - URL fixa e cliente axios padrão
 */
// Função de upload para o Panda
async function uploadVideoToPanda(folderId, title, description, videoUrl, vimeoVideoId) {
  const payload = {
    folder_id: folderId,
    title,
    description,
    url: videoUrl,
  };

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await axios.post(PANDA_UPLOAD_URL, payload, {
        headers: { Authorization: PANDA_TOKEN },
      });

      console.log(`[Panda] Vídeo "${title}" enviado com sucesso!`);

      // Extraindo dados relevantes da resposta
      const pandaVideoId = res.data?.id || null;
      const pandaWebsocketUrl = res.data?.websocket_url || null;

      // Salvando no banco
      console.log({vimeoVideoId, pandaVideoId, pandaWebsocketUrl, title})
      await saveVideoMapping(vimeoVideoId, pandaVideoId, pandaWebsocketUrl, title);

      return res.data;
    } catch (e) {
      console.error(`[Panda] Erro upload vídeo "${title}" (tentativa ${attempt}):`, e.message);
      if (attempt === 3) {
        console.error(`[Panda] Falha definitiva no upload do vídeo "${title}"`);
        return null;
      }
      await new Promise(r => setTimeout(r, 1000 * attempt));
    }
  }
}

/**
 * Processa vídeos dentro da pasta Vimeo
 */
async function processVideosInFolder(vimeoFolder, pandaFolderId) {
  const videosUri = vimeoFolder.folder?.metadata?.connections?.videos?.uri;
  if (!videosUri) {
    console.log(`[Vimeo] Pasta "${vimeoFolder.folder?.name}" sem vídeos.`);
    return;
  }

  let url = videosUri + '?per_page=100';
  do {
    const data = await safeGet(axiosVimeo, url);

    if (!data?.data || !Array.isArray(data.data)) break;

    for (const video of data.data) {
      const title = video.name || 'sem título';
      const description = video.description || '';
      let downloadUrl = null;

      if (Array.isArray(video.download) && video.download.length > 0) {
        video.download.sort((a, b) => (b.width || 0) - (a.width || 0));
        downloadUrl = video.download[0].link || null;
      }

      if (!downloadUrl) {
        console.error(`[Vimeo] Vídeo "${title}" sem link de download. Ignorando.`);
        continue;
      }

      // tenta achar no Panda um vídeo com o mesmo título ou URL original
      const existingPandaVideo = await findPandaVideoByVimeoUrl(downloadUrl);

      if (existingPandaVideo) {
        console.log(`[Match] Vídeo encontrado no Panda: ${title} -> ${existingPandaVideo.id}`);
        await saveVideoMapping(video.uri, existingPandaVideo.id, existingPandaVideo.websocket_url, title);
        continue;
      }

      // Se não achar, aí sim decide se quer fazer upload ou não
      console.warn(`[No Match] Vídeo não encontrado no Panda: ${title}`);
      // await uploadVideoToPanda(...)
    }

    url = data.paging?.next || null;
  } while (url);
}

/**
 * Processa pasta do Vimeo (recursivo)
 */
const folderMap = new Map();

async function processFolder(vimeoFolderUri, pandaParentId = null) {
  let url = vimeoFolderUri + '?per_page=100';

  do {
    const data = await safeGet(axiosVimeo, url);

    if (!data?.data || !Array.isArray(data.data)) break;

    for (const folder of data.data) {
      const folderName = folder.folder?.name || 'sem nome';
      const vimeoUri = folder.folder?.uri;

      if (!vimeoUri) continue;

      if (folderMap.has(vimeoUri)) {
        const pandaFolderId = folderMap.get(vimeoUri);
        console.log(`[Cache] Pasta já mapeada: "${folderName}" (ID Panda: ${pandaFolderId})`);

        await processVideosInFolder(folder, pandaFolderId);

        const subfoldersUri = folder.folder?.metadata?.connections?.items?.uri;
        if (subfoldersUri) {
          await processFolder(subfoldersUri, pandaFolderId);
        }
      } else {
        let pandaFolderId = await findPandaFolder(folderName, pandaParentId);

        if (!pandaFolderId) {
          pandaFolderId = await createPandaFolder(folderName, pandaParentId);
          if (!pandaFolderId) {
            console.error(`[Erro] Não foi possível criar pasta "${folderName}". Pulando.`);
            continue;
          }
        }

        folderMap.set(vimeoUri, pandaFolderId);

        await processVideosInFolder(folder, pandaFolderId);

        const subfoldersUri = folder.folder?.metadata?.connections?.items?.uri;
        if (subfoldersUri) {
          await processFolder(subfoldersUri, pandaFolderId);
        }
      }
    }

    url = data.paging?.next || null;
  } while (url);
}

/**
 * Função principal para rodar o script manualmente
 */
export async function run() {
  try {
    const rootUri = `/users/${VIMEO_USER_ID}/folders/root?direction=asc&exclude_personal_team_folder=true&exclude_shared_videos=false&no_padding=true&sort=alphabetical&responsive=true&fields=video.allowed_privacies,video.app.uri,video.can_move_to_project,video.config_url,video.created_time,video.duration,video.download.link,video.download.type,video.download.width,video.download.height,video.download.quality,video.download.size,video.download.public_name,video.download.size_short,video.embed.html,video.files_size,video.last_user_action_event_date,video.link,video.manage_link,video.metadata.can_be_replaced,video.metadata.interactions.edit.uri,video.metadata.interactions.delete.uri,video.metadata.interactions.invite.uri,video.metadata.interactions.legal_hold.uri,video.modified_time,video.name,video.pictures.default_picture,video.pictures.uri,video.pictures.sizes,video.password,video.privacy,video.regional_privacies,video.release_time,video.review_page,video.status,video.uploader.pictures,video.uri,video.user.account,video.user.uri,folder.created_time,folder.last_user_action_event_date,folder.name,folder.uri,folder.privacy,folder.is_pinned,folder.is_private_to_user,folder.is_slack_notification_enabled,folder.metadata.connections.items.uri,folder.metadata.connections.items.total,folder.metadata.connections.parent_folder.uri,folder.metadata.connections.videos.uri,folder.metadata.connections.team_members.uri,folder.settings,folder.metadata.interactions.edit,folder.metadata.interactions.edit_settings,folder.metadata.interactions.delete,folder.metadata.interactions.invite,folder.metadata.interactions.move_video,folder.user.uri,folder.use_parent_slack_settings,folder.slack_incoming_webhooks_id,type`;

    await processFolder(rootUri);
    console.log('Processamento finalizado!');
  } catch (e) {
    console.error('Erro fatal:', e);
  }
}

function formatVimeoUrl(path) {
  const match = path.match(/^\/videos\/(\d+)$/);
  if (!match) {
    throw new Error("Formato inválido. Esperado: /videos/{id}");
  }
  const videoId = match[1];
  return `https://player.vimeo.com/video/${videoId}`;
}

async function videoExistsInDB(vimeoVideoId) {
  const sql = 'SELECT 1 FROM vimeo_panda_videos WHERE vimeo_video_id = ? LIMIT 1';
  const [rows] = await pool.execute(sql, [formatVimeoUrl(vimeoVideoId)]);
  return rows.length > 0;
}

await run()
