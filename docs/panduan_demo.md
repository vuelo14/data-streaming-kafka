# Panduan Rekaman Demo Video (End-to-End)

Dokumen ini berisi skenario panduan langkah demi langkah untuk merekam video demonstrasi tugas besar Big Data "Real-Time E-Commerce Clickstream Pipeline". Video ini berdurasi optimal antara **7-10 menit** sesuai dengan ketentuan tugas.

---

## 🎬 Persiapan Sebelum Merekam

1. **Pastikan Docker Sudah Berjalan:**
   - Buka WSL atau Terminal, arahkan ke folder proyek.
   - Jalankan `docker compose up -d --build`.
   - Pastikan status semua container "Up" dengan perintah `docker compose ps`.

2. **Siapkan Tampilan Layar (Screen Layout):**
   Untuk menunjukkan kesan *real-time*, disarankan merekam seluruh layar penuh (full screen) dengan pembagian jendela sebagai berikut:
   - **Kiri (60% layar):** Browser Web menampilkan Dashboard Streamlit (`http://localhost:8501`).
   - **Kanan Atas (40% layar):** Terminal 1 untuk Producer log.
   - **Kanan Bawah (40% layar):** Terminal 2 untuk Processor log.

3. **Buka Terminal Logs:**
   - Di Terminal Kanan Atas: ketik `docker compose logs -f producer` (tekan Enter).
   - Di Terminal Kanan Bawah: ketik `docker compose logs -f processor` (tekan Enter).

---

## 📝 Skenario Perekaman (Timeline)

### 1. Pembukaan & Latar Belakang (00:00 - 01:30)
- **Visual:** Tampilkan wajah Anda / tim (opsional), lalu tampilkan file `README.md` atau `docs/arsitektur.md` secara singkat.
- **Narasi:** 
  - "Halo, kami dari kelompok / saya Wawan."
  - Toto, Ahmadi.
  - "Studi kasus kami adalah E-Commerce Clickstream Analytics menggunakan Apache Kafka."
  - "Tujuan sistem ini adalah untuk menangkap data aktivitas pengunjung web (seperti view produk, add to cart, dan purchase) secara real-time, lalu memprosesnya untuk mendapatkan trending products, mendeteksi bot, dan menampilkannya di live dashboard."

### 2. Penjelasan Arsitektur (01:30 - 03:00)
- **Visual:** Tampilkan diagram arsitektur.
- **Narasi:**
  - Jelaskan alur data secara ringkas: "Data dikirim oleh **Producer Python** ke **Kafka**, lalu diambil oleh **Stream Processor Python**. Processor melakukan *Filtering* (deteksi bot), *Aggregation* (view per produk), dan *Enrichment* (gabung dengan data user statis). Hasilnya disimpan di **PostgreSQL** dan ditampilkan oleh **Streamlit**."
  - Sebutkan bahwa Kafka menggunakan 3 partisi dan 1 replikasi.

### 3. Menjalankan Pipeline & Menunjukkan Logs (03:00 - 05:00)
- **Visual:** Pindah ke layout Split Screen (Browser di kiri, 2 Terminal di kanan).
- **Narasi:**
  - "Di sebelah kanan ini adalah log dari Producer dan Processor yang berjalan secara *background* menggunakan Docker."
  - Tunjuk Terminal Kanan Atas (Producer): "Di sini kita bisa melihat Python Producer sedang mensimulasikan sekitar 25 event/detik ke dalam Kafka."
  - Tunjuk Terminal Kanan Bawah (Processor): "Di bawah ini, Processor secara kontinu membaca dari topik Kafka. Processor melakukan deteksi aktivitas mencurigakan dan menyatukannya ke tabel database. Terlihat statistik jumlah yang diproses selalu bertambah."

### 4. Demonstrasi Dashboard Real-Time (05:00 - 08:30)
- **Visual:** Fokus pada browser Streamlit (di sebelah kiri). Anda dapat *maximize* jendela browser jika diperlukan.
- **Narasi & Aksi:**
  - **Metrics Utama:** "Perhatikan angka di Metric Cards ini terus bertambah secara real-time setiap 5 detik tanpa harus me-refresh manual."
  - **Trending Products:** "Grafik batang di sini adalah hasil dari fitur **Aggregation** (Windowing). Ia menghitung mana produk yang paling banyak dilihat dalam waktu 10 menit terakhir."
  - **Suspicious Activity:** Scroll ke bawah bagian Alert. "Di sini kami menerapkan fitur **Filtering**. Jika ada user yang mengirim event berlebihan (misalnya akun bot yang kami simulasikan), ia akan terdeteksi di sini."
  - **Live Feed:** Tunjukkan tabel paling bawah. "Tabel ini menunjukkan *enrichment data*, di mana ID user sudah otomatis diubah menjadi nama lengkap dan tier membershipnya berdasarkan data statis kita."

### 5. Penutup (08:30 - 09:00)
- **Visual:** Kembali ke layar penuh / wajah.
- **Narasi:** 
  - "Sekian demonstrasi pipeline streaming E-Commerce kami."
  - Sebutkan sedikit tantangan teknis (misal: "Tantangan terbesar adalah menangani auto-refresh pada Streamlit tanpa membebani database, namun terselesaikan.").
  - Ucapkan terima kasih dan tutup video.

---

## 💡 Tips Tambahan
- Gunakan software perekam layar seperti **OBS Studio** karena sangat mudah untuk merekam layar *desktop* secara penuh.
- Berlatih narasi 1-2 kali sebelum menekan tombol record agar durasi tidak melewati batas 10 menit.
- Suara/audio sangat penting. Pastikan Anda merekam di tempat yang tenang dengan mic yang jelas.
- Jika ada kesalahan kecil di tengah jalan (misalnya salah klik), teruskan saja bicara, tidak perlu diulangi dari awal selama esensi sistemnya terlihat berfungsi.
