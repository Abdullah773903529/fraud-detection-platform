#!/bin/bash
# ============================================
# تهيئة مواضيع كافكا لنظام كشف الاحتيال
# ============================================

echo "🚀 بدء تهيئة مواضيع كافكا..."

# ============================================
# 1. انتظار حتى يصبح كافكا جاهزاً
# ============================================
echo "⏳ انتظار كافكا ليكون جاهزاً..."
sleep 15

# ============================================
# 2. إنشاء المواضيع
# ============================================

# الموضوع 1: المعاملات الخام
echo "📝 إنشاء topic: raw_transactions"
docker exec kafka1 kafka-topics --create \
  --if-not-exists \
  --topic raw_transactions \
  --bootstrap-server localhost:29092 \
  --partitions 6 \
  --replication-factor 3

# الموضوع 2: المعاملات المنظفة
echo "📝 إنشاء topic: cleaned_transactions"
docker exec kafka1 kafka-topics --create \
  --if-not-exists \
  --topic cleaned_transactions \
  --bootstrap-server localhost:29092 \
  --partitions 4 \
  --replication-factor 3

# الموضوع 3: تنبيهات الاحتيال
echo "📝 إنشاء topic: fraud_alerts"
docker exec kafka1 kafka-topics --create \
  --if-not-exists \
  --topic fraud_alerts \
  --bootstrap-server localhost:29092 \
  --partitions 2 \
  --replication-factor 3

# ============================================
# 3. عرض المواضيع للتأكد
# ============================================
echo ""
echo "✅ تم إنشاء المواضيع التالية:"
docker exec kafka1 kafka-topics --list --bootstrap-server localhost:29092

echo ""
echo "📊 تفاصيل الموضوع raw_transactions:"
docker exec kafka1 kafka-topics --describe \
  --topic raw_transactions \
  --bootstrap-server localhost:29092

echo ""
echo "✅ اكتملت تهيئة كافكا بنجاح!"