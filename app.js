const express = require('express');
const { NodeSSH } = require('node-ssh');
const fs = require('fs').promises;
const path = require('path');
const cron = require('node-cron');
const os = require('os');
const { Pool } = require('pg');

const app = express();
const port = 3110;

// Konfigurasi SSH
const SSH_CONFIG_FH = {
  host: '172.17.32.128',
  username: 'root',
  password: 'myr3pGOP6',
};

const SSH_CONFIG_SVN = {
  host: '172.17.12.215',
  username: 'backend',
  password: 'Myrep123!',
};

// PostgreSQL connection configuration
const pgConfig = {
  user: 'noc',
  host: '172.17.76.36',
  database: 'nisa',
  password: 'myrep123!',
  port: 5432,
  connectionTimeoutMillis: 30000
};

// Create a new pool instance
const pool = new Pool(pgConfig);

// Function to log transfer attempt with upsert logic
async function logTransfer(sourceIp, sourceFile, destinationHostname, status, errorMessage = null, durationMs = null) {
  let client;
  
  try {
    client = await pool.connect();
    
    // Use upsert (INSERT ... ON CONFLICT ... UPDATE) pattern
    const query = `
      INSERT INTO observium_rancid_cfg_sync_auto_logs_fh
        (source_ip, source_file, destination_hostname, status, error_message, transfer_duration_ms, timestamp)
      VALUES
        ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
      ON CONFLICT (source_ip)
      DO UPDATE SET
        source_file = $2,
        destination_hostname = $3,
        status = $4,
        error_message = $5,
        transfer_duration_ms = $6,
        timestamp = CURRENT_TIMESTAMP
      RETURNING id
    `;
    
    const result = await client.query(query, [
      sourceIp,
      sourceFile,
      destinationHostname,
      status,
      errorMessage,
      durationMs
    ]);
    
    if (result.rows.length > 0) {
      console.log(`Log entry ${result.rows[0].id} updated/created for ${sourceFile}`);
    }
  } catch (error) {
    console.error('Error logging to database:', error);
  } finally {
    if (client) {
      client.release();
    }
  }
}

// Fungsi untuk mendapatkan file konfigurasi terbaru untuk setiap IP
async function getLatestConfigFilesForEachIP(ssh) {
  try {
    // Masuk sebagai root
    await ssh.execCommand('sudo -i', { stdin: '\n' });
    
    // Mendapatkan daftar file di direktori /home/nmsfh/Policy_Task/Config_Export dengan format timestamp lengkap
    const result = await ssh.execCommand('ls -la --time-style=full-iso /home/nmsfh/Policy_Task/Config_Export', { cwd: '/' });
    
    if (result.stderr) {
      console.error('Error listing files:', result.stderr);
      return [];
    }
    
    // Regex untuk mencocokkan format config_ip_bulantanggal
    const configRegex = /config_(\d+\.\d+\.\d+\.\d+)_(\d{4})/;
    
    // Kelompokkan file berdasarkan IP
    const filesByIP = {};
    
    // Parse output ls -la
    const lines = result.stdout.split('\n');
    
    for (const line of lines) {
      const parts = line.trim().split(/\s+/);
      if (parts.length < 9) continue; // Bukan baris file yang valid
      
      // Format dari ls -la --time-style=full-iso akan membuat timestamp berada di posisi 6-7
      const dateStr = `${parts[5]} ${parts[6]}`;
      const fileName = parts[parts.length - 1];
      
      const match = fileName.match(configRegex);
      
      if (match) {
        const ip = match[1];
        const fileDate = new Date(dateStr);
        
        // Cek apakah sudah ada file untuk IP ini dan apakah file ini lebih baru
        if (!filesByIP[ip] || filesByIP[ip].date < fileDate) {
          filesByIP[ip] = {
            fileName: fileName,
            date: fileDate
          };
        }
      }
    }
    
    // Kembalikan array dari file terbaru untuk setiap IP
    return Object.entries(filesByIP).map(([ip, fileInfo]) => ({
      ip,
      fileName: fileInfo.fileName
    }));
  } catch (error) {
    console.error('Error getting latest configuration files:', error);
    return [];
  }
}

// Fungsi untuk mendapatkan hostname dari IP menggunakan database PostgreSQL
async function getHostnameForIP(ip) {
  let client;
  
  try {
    // Mendapatkan koneksi dari pool PostgreSQL
    client = await pool.connect();
    
    // Query untuk mencari hostname berdasarkan IP di tabel observium_svn_comparison
    const query = `
      SELECT ip, hostname
      FROM observium_svn_comparison
      WHERE ip = $1
    `;
    
    // Eksekusi query dengan IP sebagai parameter
    const result = await client.query(query, [ip]);
    
    // Periksa apakah data ditemukan
    if (result.rows.length > 0) {
      // Data ditemukan, kembalikan hostname dalam format lowercase
      console.log(`Hostname for IP ${ip} found in database: ${result.rows[0].hostname}`);
      return result.rows[0].hostname.toLowerCase();
    } else {
      // Tidak ada data yang cocok untuk IP tersebut
      console.error(`IP ${ip} not found in database`);
      return null;
    }
  } catch (error) {
    // Tangani kesalahan yang mungkin terjadi saat menjalankan query
    console.error(`Error getting hostname for IP ${ip} from database:`, error);
    return null;
  } finally {
    // Pastikan untuk melepaskan koneksi kembali ke pool
    if (client) {
      client.release();
    }
  }
}

// Fungsi utama untuk memindahkan file dari server FH ke server SVN
async function transferConfigFiles() {
  const sshFH = new NodeSSH();
  const sshSVN = new NodeSSH();
  const tempDir = path.join(os.tmpdir(), 'config-transfer-fh');
  
  try {
    // Membuat direktori temp jika belum ada
    await fs.mkdir(tempDir, { recursive: true });
    
    // Connect ke server FH
    console.log('Connecting to FiberHome server...');
    await sshFH.connect(SSH_CONFIG_FH);
    console.log('Connected to FiberHome server');
    
    // Mendapatkan daftar file terbaru untuk setiap IP
    const latestFiles = await getLatestConfigFilesForEachIP(sshFH);
    console.log(`Found ${latestFiles.length} latest configuration files`);
    
    if (latestFiles.length === 0) {
      console.log('No configuration files found, exiting');
      return;
    }
    
    // Connect ke server SVN
    console.log('Connecting to SVN server...');
    await sshSVN.connect(SSH_CONFIG_SVN);
    console.log('Connected to SVN server');
    
    // Proses setiap file
    for (const fileInfo of latestFiles) {
      try {
        const startTime = Date.now();
        const { ip, fileName } = fileInfo;
        const localFilePath = path.join(tempDir, fileName);
        
        // Download file dari server FH
        console.log(`Downloading ${fileName} from FiberHome server...`);
        await sshFH.getFile(localFilePath, `/home/nmsfh/Policy_Task/Config_Export/${fileName}`);
        
        // Dapatkan hostname untuk IP ini dari database
        const hostname = await getHostnameForIP(ip);
        
        if (!hostname) {
          const errorMsg = `Couldn't find hostname for IP ${ip} in database, skipping file ${fileName}`;
          console.error(errorMsg);
          await logTransfer(ip, fileName, null, 'FAILED', errorMsg, Date.now() - startTime);
          continue;
        }
        
        // Upload file ke server SVN dengan nama baru
        console.log(`Uploading config to SVN server as ${hostname}...`);
        await sshSVN.putFile(localFilePath, `/opt/observium/rancid/${hostname}`);
        console.log(`Successfully transferred ${fileName} to ${hostname}`);
        
        // Log successful transfer
        await logTransfer(ip, fileName, hostname, 'SUCCESS', null, Date.now() - startTime);
        
        // Hapus file lokal
        await fs.unlink(localFilePath);
      } catch (fileError) {
        console.error(`Error processing file ${fileInfo?.fileName}:`, fileError);
        await logTransfer(fileInfo?.ip, fileInfo?.fileName, null, 'FAILED', fileError.message, null);
      }
    }
    
    console.log('File transfer completed');
  } catch (error) {
    console.error('Error in transfer process:', error);
    await logTransfer(null, 'process', null, 'FAILED', `General process error: ${error.message}`, null);
  } finally {
    // Tutup koneksi SSH
    sshFH.dispose();
    sshSVN.dispose();
    
    // Coba hapus direktori temp
    try {
      await fs.rm(tempDir, { recursive: true });
    } catch (e) {
      console.error('Failed to remove temp directory:', e);
    }
  }
}

// Jadwalkan tugas untuk berjalan setiap 24 jam
cron.schedule('0 0 * * *', () => {
  console.log('Running scheduled configuration transfer task');
  transferConfigFiles();
});

// Endpoint untuk memulai transfer secara manual
app.get('/trigger-transfer', (req, res) => {
  console.log('Transfer triggered manually');
  transferConfigFiles();
  res.send('Configuration transfer process started');
});

// Endpoint untuk melihat log transfer terakhir
app.get('/logs', async (req, res) => {
  try {
    const client = await pool.connect();
    const result = await client.query('SELECT * FROM observium_rancid_cfg_sync_auto_logs_fh ORDER BY timestamp DESC LIMIT 100');
    client.release();
    
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching logs:', error);
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

// Jalankan aplikasi
app.listen(port, async () => {
  console.log(`Server running on port ${port}`);
  
  console.log('Running initial configuration transfer');
  transferConfigFiles();
});