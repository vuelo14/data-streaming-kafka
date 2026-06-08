​Tugas Besar Big Data: Real-Time Data Streaming Pipeline dengan Apache Kafka

​📌 Deskripsi Tugas

​Dalam tugas ini, kalian diminta untuk merancang dan mengimplementasikan sebuah sistem data streaming yang mampu menerima data secara real-time, memprosesnya secara instan, dan menyimpannya ke dalam database atau menampilkannya dalam bentuk dashboard interaktif.

​🛠️ Arsitektur Sistem & Teknologi

​Kalian bebas memilih studi kasus (misal: streaming transaksi finansial untuk deteksi fraud, data sensor IoT, log aktivitas web, atau tweet media sosial). Namun, arsitektur wajib memenuhi komponen berikut:

​Data Source & Producer: Mensimulasikan atau mengambil data asli secara real-time dan mengirimkannya ke Apache Kafka.

​Message Broker (Apache Kafka): Mengelola stream data menggunakan Topic, Partition, dan Replication Factor yang tepat.

​Stream Processor (Pilih salah satu): \* Kafka Streams, ​Apache Spark Streaming, ​Apache Flink, ​Python (menggunakan library kafka-python atau confluent-kafka untuk pemrosesan sederhana)

​Data Sink & Consumer: Menyimpan data hasil pemrosesan ke database (misal: MongoDB, PostgreSQL, Elasticsearch) atau langsung meneruskannya ke dashboard (misal: Grafana, Kibana, Streamlit).

​📋 Komponen Penilaian & Requirement :

1. ​Desain Arsitektur & Topologi Kafka (Bobot: 20%)
   Membuat diagram arsitektur sistem yang jelas.
   Menentukan jumlah Topic, Partition, dan Consumer Group disertai alasan yang logis demi efisiensi dan scalability.
2. Implementasi Producer (Bobot: 25%)
   Mengirimkan data dalam format terstruktur (diutamakan JSON atau Avro).
   Data harus dikirim secara kontinu (misal: minimal 10-50 pesan per detik) untuk mensimulasikan kondisi real-time yang sebenarnya.
3. Stream Processing & Analytics (Bobot: 30%)
   Data tidak boleh hanya sekadar lewat. Harus ada proses transformasi atau analisis, seperti: 
   Filtering: Menyaring data yang tidak valid atau mencurigakan.
   Aggregation: Menghitung total, rata-rata, atau windowing (misal: jumlah transaksi per 5 menit).
   Enrichment: Menggabungkan data stream dengan data statis (misal: mencocokkan ID user dengan data profil).
4. Storage & Visualization / Dashboard (Bobot: 25%)
   Menampilkan data yang sudah diproses ke dalam dashboard yang memperbarui tampilannya secara otomatis (auto-refresh / live update).

​📂 Luaran (Deliverables) yang Wajib Dikumpulkan :

1. ​Kode Sumber (Repository GitHub):
   Pastikan menyertakan file README.md yang berisi panduan cara menjalankan sistem dari awal hingga selesai.
   Gunakan docker-compose.yml untuk mempermudah dosen/asisten dalam menjalankan Apache Kafka dan komponen pendukungnya.
2. Laporan Resmi (PDF):
   \- Latar belakang studi kasus yang dipilih.
   \- Diagram arsitektur.
   \- Skema data (skema JSON/Avro).
   \- Analisis performa sistem (kendala yang dihadapi, throughput, atau latency jika ada).
3. Demo Video (Link YouTube/Drive):
   Durasi maksimal 7-10 menit yang menunjukkan sistem berjalan secara end-to-end (mulai dari data dikirim oleh producer hingga muncul di dashboard).

