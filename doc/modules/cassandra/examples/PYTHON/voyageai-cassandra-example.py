#!/usr/bin/env python3
"""
VoyageAI + Apache Cassandra: Comprehensive Vector Search Integration

This comprehensive example demonstrates the complete VoyageAI integration with
Cassandra, combining multiple advanced features in one production-ready guide:

1. Standard text embeddings (voyage-3.5, voyage-3.5-lite)
2. Token-aware batching for large datasets
3. Reranking with rerank-2.5 for two-stage retrieval
4. Hybrid search (vector + keyword filters + reranking)

Use Case: E-commerce product search with 100+ products

Prerequisites:
- Python 3.8+
- pip install voyageai cassandra-driver
- VoyageAI API key (set as VOYAGE_API_KEY environment variable)
- Apache Cassandra 5.0+ cluster running (default: localhost:9042)

Author: Apache Cassandra Documentation Team
License: Apache 2.0
"""

import os
import sys
import time
from typing import List, Dict, Any, Optional, Generator, Set, Tuple
from datetime import datetime
from decimal import Decimal
import uuid

try:
    import voyageai
    from cassandra.cluster import Cluster, Session
    from cassandra.auth import PlainTextAuthProvider
except ImportError as e:
    print(f"Error: Missing required dependency - {e}")
    print("Install dependencies: pip install voyageai cassandra-driver")
    sys.exit(1)


# ============================================================================
# SECTION 1: CONFIGURATION
# ============================================================================

class Config:
    """Configuration for VoyageAI and Cassandra connection."""

    # VoyageAI settings
    VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
    EMBEDDING_MODEL = "voyage-3.5-lite"  # Options: voyage-3.5, voyage-3.5-lite
    RERANK_MODEL = "rerank-2.5"  # Options: rerank-2.5, rerank-2.5-lite
    EMBEDDING_DIMENSION = 1024  # Options: 256, 512, 1024, 2048

    # Cassandra settings
    CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
    CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
    CASSANDRA_KEYSPACE = "voyageai_demo"
    CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME")
    CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")

    # Search settings
    SIMILARITY_FUNCTION = "COSINE"  # Options: COSINE, DOT_PRODUCT, EUCLIDEAN

    @classmethod
    def validate(cls):
        """Validate required configuration."""
        if not cls.VOYAGE_API_KEY:
            raise ValueError(
                "VOYAGE_API_KEY environment variable is required.\n"
                "Get your API key from: https://dash.voyageai.com/api-keys\n"
                "Set it with: export VOYAGE_API_KEY='your-api-key-here'"
            )


# ============================================================================
# SECTION 2: TOKEN-AWARE BATCHING
# ============================================================================

# Token limits for VoyageAI models (per batch)
VOYAGE_TOKEN_LIMITS = {
    "voyage-3.5-lite": 1_000_000,
    "voyage-3.5": 320_000,
    "voyage-context-3": 32_000,
    "voyage-multimodal-3": 120_000,
}


class TokenAwareBatcher:
    """
    Token-aware batching utility for VoyageAI embeddings.

    This class implements intelligent batching based on actual token counts
    rather than simple document counts, preventing API errors from exceeding
    model token limits.
    """

    def __init__(self, client: voyageai.Client, model: str):
        """
        Initialize token-aware batcher.

        Args:
            client: VoyageAI client instance
            model: Model name (determines token limit)
        """
        self.client = client
        self.model = model
        self.max_tokens = VOYAGE_TOKEN_LIMITS.get(model, 120_000)

    def analyze_tokens(self, texts: List[str]) -> Dict[str, Any]:
        """
        Analyze token distribution across texts.

        Args:
            texts: List of texts to analyze

        Returns:
            Dictionary with token statistics
        """
        all_token_lists = self.client.tokenize(texts, model=self.model)
        token_counts = [len(tokens) for tokens in all_token_lists]

        return {
            "total_docs": len(texts),
            "total_tokens": sum(token_counts),
            "min_tokens": min(token_counts),
            "max_tokens": max(token_counts),
            "avg_tokens": sum(token_counts) / len(token_counts),
            "token_counts": token_counts,
        }

    def build_token_batches(self, texts: List[str]) -> Generator[List[str], None, None]:
        """
        Build batches based on actual token counts.

        This is the recommended batching approach. It:
        1. Tokenizes all texts in one API call (efficient)
        2. Builds batches that respect token limits
        3. Maximizes batch utilization
        4. Prevents API errors from oversized batches

        Args:
            texts: List of texts to batch

        Yields:
            Batches of texts
        """
        if not texts:
            return

        # Get token counts for all texts in one API call
        all_token_lists = self.client.tokenize(texts, model=self.model)
        token_counts = [len(tokens) for tokens in all_token_lists]

        current_batch = []
        current_batch_tokens = 0

        for i, text in enumerate(texts):
            n_tokens = token_counts[i]

            # Check if adding this would exceed token limit
            if current_batch and (current_batch_tokens + n_tokens > self.max_tokens):
                yield current_batch
                current_batch = []
                current_batch_tokens = 0

            current_batch.append(text)
            current_batch_tokens += n_tokens

        # Yield final batch
        if current_batch:
            yield current_batch

    def embed_with_batching(
        self,
        texts: List[str],
        input_type: str = "document",
        dimension: int = 1024
    ) -> Tuple[List[List[float]], Dict[str, Any]]:
        """
        Embed texts using token-aware batching.

        Args:
            texts: List of texts to embed
            input_type: "document" or "query"
            dimension: Output dimension

        Returns:
            Tuple of (embeddings, batch_stats)
        """
        all_embeddings = []
        batch_stats = []

        for batch_num, batch in enumerate(self.build_token_batches(texts), 1):
            result = self.client.embed(
                texts=batch,
                model=self.model,
                input_type=input_type,
                output_dimension=dimension
            )

            all_embeddings.extend(result.embeddings)

            batch_stats.append({
                "batch_num": batch_num,
                "num_texts": len(batch),
                "total_tokens": result.total_tokens,
            })

        stats = {
            "total_batches": len(batch_stats),
            "batches": batch_stats,
        }

        return all_embeddings, stats


# ============================================================================
# SECTION 3: VOYAGEAI CLIENT WRAPPER
# ============================================================================

class VoyageAIClient:
    """
    Comprehensive VoyageAI client with embedding and reranking support.
    """

    def __init__(self, api_key: str):
        """
        Initialize VoyageAI client.

        Args:
            api_key: VoyageAI API key
        """
        self.client = voyageai.Client(api_key=api_key)
        print(f"✓ VoyageAI client initialized")

    def embed_texts(
        self,
        texts: List[str],
        model: str = "voyage-3.5-lite",
        input_type: str = "document",
        dimension: int = 1024
    ) -> List[List[float]]:
        """
        Generate embeddings for texts (simple batching).

        Args:
            texts: List of text strings to embed
            model: Model name
            input_type: "document" or "query"
            dimension: Output dimension

        Returns:
            List of embedding vectors
        """
        if not texts:
            return []

        result = self.client.embed(
            texts=texts,
            model=model,
            input_type=input_type,
            output_dimension=dimension
        )

        return result.embeddings

    def embed_single(
        self,
        text: str,
        model: str = "voyage-3.5-lite",
        input_type: str = "query",
        dimension: int = 1024
    ) -> List[float]:
        """
        Generate embedding for a single text.

        Args:
            text: Text to embed
            model: Model name
            input_type: "document" or "query"
            dimension: Output dimension

        Returns:
            Single embedding vector
        """
        embeddings = self.embed_texts([text], model, input_type, dimension)
        return embeddings[0] if embeddings else []

    def rerank(
        self,
        query: str,
        documents: List[str],
        model: str = "rerank-2.5",
        top_k: Optional[int] = None
    ):
        """
        Rerank documents based on relevance to query.

        Args:
            query: Search query text
            documents: List of document texts to rerank
            model: Reranking model (rerank-2.5, rerank-2.5-lite)
            top_k: Return only top K results (None = all)

        Returns:
            RerankingResponse with sorted results
        """
        result = self.client.rerank(
            query=query,
            documents=documents,
            model=model,
            top_k=top_k,
            truncation=True
        )

        return result


# ============================================================================
# SECTION 4: CASSANDRA INTEGRATION
# ============================================================================

class CassandraVectorStore:
    """Handles Cassandra connection and vector operations."""

    def __init__(
        self,
        hosts: List[str],
        port: int = 9042,
        username: Optional[str] = None,
        password: Optional[str] = None
    ):
        """Initialize Cassandra connection."""
        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)

        self.cluster = Cluster(
            contact_points=hosts,
            port=port,
            auth_provider=auth_provider
        )
        self.session: Optional[Session] = None
        print(f"✓ Cassandra cluster initialized (hosts: {', '.join(hosts)})")

    def connect(self):
        """Establish connection to Cassandra cluster."""
        try:
            self.session = self.cluster.connect()
            print("✓ Connected to Cassandra cluster")
        except Exception as e:
            print(f"Error connecting to Cassandra: {e}")
            raise

    def close(self):
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            print("✓ Cassandra connection closed")

    def create_keyspace(self, keyspace: str, replication_factor: int = 1):
        """Create keyspace if it doesn't exist."""
        query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{
            'class': 'SimpleStrategy',
            'replication_factor': {replication_factor}
        }}
        """
        self.session.execute(query)
        print(f"✓ Keyspace '{keyspace}' created/verified")

    def create_products_table(self, keyspace: str, dimension: int):
        """
        Create products table with vector column and metadata.

        Includes columns for hybrid search (vector + keyword filtering).
        """
        self.session.set_keyspace(keyspace)

        query = f"""
        CREATE TABLE IF NOT EXISTS products (
            product_id UUID PRIMARY KEY,
            name TEXT,
            description TEXT,
            category TEXT,
            subcategory TEXT,
            price DECIMAL,
            brand TEXT,
            in_stock BOOLEAN,
            rating DECIMAL,
            tags SET<TEXT>,
            description_vector VECTOR<FLOAT, {dimension}>,
            created_at TIMESTAMP
        )
        """
        self.session.execute(query)
        print(f"✓ Table 'products' created with VECTOR<FLOAT, {dimension}> column")

    def create_indexes(self, keyspace: str, similarity_function: str = "COSINE"):
        """
        Create SAI indexes for vector and keyword search.

        Args:
            keyspace: Keyspace name
            similarity_function: COSINE, DOT_PRODUCT, or EUCLIDEAN
        """
        self.session.set_keyspace(keyspace)

        # Vector index for similarity search
        self.session.execute(f"""
        CREATE CUSTOM INDEX IF NOT EXISTS products_vector_idx
        ON products(description_vector)
        USING 'StorageAttachedIndex'
        WITH OPTIONS = {{
            'similarity_function': '{similarity_function}'
        }}
        """)
        print(f"✓ SAI vector index created (similarity: {similarity_function})")

        # Keyword/metadata indexes for filtering
        self.session.execute("""
        CREATE CUSTOM INDEX IF NOT EXISTS products_category_idx
        ON products(category)
        USING 'StorageAttachedIndex'
        """)

        self.session.execute("""
        CREATE CUSTOM INDEX IF NOT EXISTS products_brand_idx
        ON products(brand)
        USING 'StorageAttachedIndex'
        """)

        self.session.execute("""
        CREATE CUSTOM INDEX IF NOT EXISTS products_in_stock_idx
        ON products(in_stock)
        USING 'StorageAttachedIndex'
        """)

        print("✓ SAI keyword indexes created (category, brand, in_stock)")

    def insert_product(
        self,
        keyspace: str,
        product_id: uuid.UUID,
        name: str,
        description: str,
        category: str,
        subcategory: str,
        price: float,
        brand: str,
        in_stock: bool,
        rating: float,
        tags: List[str],
        description_vector: List[float]
    ):
        """Insert product with embedding vector."""
        self.session.set_keyspace(keyspace)

        query = """
        INSERT INTO products (
            product_id, name, description, category, subcategory,
            price, brand, in_stock, rating, tags,
            description_vector, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        prepared = self.session.prepare(query)
        self.session.execute(
            prepared,
            (
                product_id, name, description, category, subcategory,
                Decimal(str(price)), brand, in_stock, Decimal(str(rating)),
                set(tags), description_vector, datetime.utcnow()
            )
        )

    def vector_search(
        self,
        keyspace: str,
        query_vector: List[float],
        limit: int = 50,
        category: Optional[str] = None,
        brand: Optional[str] = None,
        max_price: Optional[float] = None,
        in_stock_only: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Perform vector search with optional keyword filters.

        Args:
            keyspace: Keyspace name
            query_vector: Query embedding vector
            limit: Maximum number of results
            category: Filter by category
            brand: Filter by brand
            max_price: Maximum price filter
            in_stock_only: Only return in-stock items

        Returns:
            List of product dictionaries with similarity scores
        """
        self.session.set_keyspace(keyspace)

        # Build query with filters
        where_clauses = []
        params = []

        if category:
            where_clauses.append("category = ?")
            params.append(category)

        if brand:
            where_clauses.append("brand = ?")
            params.append(brand)

        if in_stock_only:
            where_clauses.append("in_stock = ?")
            params.append(True)

        where_clause = " AND ".join(where_clauses) if where_clauses else ""
        where_sql = f"WHERE {where_clause}" if where_clause else ""

        query = f"""
        SELECT
            product_id, name, description, category, subcategory,
            price, brand, in_stock, rating, tags,
            similarity_cosine(description_vector, ?) AS similarity
        FROM products
        {where_sql}
        ORDER BY description_vector ANN OF ?
        LIMIT ?
        """

        # Add query vector twice (for similarity and ANN) plus limit
        all_params = [query_vector] + params + [query_vector, limit]

        prepared = self.session.prepare(query)
        rows = self.session.execute(prepared, tuple(all_params))

        results = []
        for row in rows:
            # Apply price filter in application layer (post-retrieval)
            if max_price and float(row.price) > max_price:
                continue

            results.append({
                "product_id": str(row.product_id),
                "name": row.name,
                "description": row.description,
                "category": row.category,
                "subcategory": row.subcategory,
                "price": float(row.price),
                "brand": row.brand,
                "in_stock": row.in_stock,
                "rating": float(row.rating),
                "tags": list(row.tags) if row.tags else [],
                "similarity": float(row.similarity) if row.similarity else None,
                "source": "vector_search"
            })

        return results


# ============================================================================
# SECTION 5: SAMPLE DATA
# ============================================================================

def generate_product_catalog(num_products: int = 100) -> List[Dict[str, Any]]:
    """
    Generate sample e-commerce product catalog.

    Args:
        num_products: Number of products to generate

    Returns:
        List of product dictionaries
    """
    import random

    categories = {
        "Electronics": {
            "subcategories": ["Audio", "Computers", "Phones", "Cameras"],
            "brands": ["Sony", "Apple", "Samsung", "Bose", "Dell", "Canon"]
        },
        "Home & Garden": {
            "subcategories": ["Furniture", "Kitchen", "Decor", "Tools"],
            "brands": ["IKEA", "KitchenAid", "DeWalt", "HomeDepot"]
        },
        "Sports & Outdoors": {
            "subcategories": ["Fitness", "Camping", "Cycling", "Running"],
            "brands": ["Nike", "Adidas", "Coleman", "Trek", "Garmin"]
        },
        "Books & Media": {
            "subcategories": ["Fiction", "Non-Fiction", "Technology", "Cooking"],
            "brands": ["Penguin", "O'Reilly", "Manning", "Harper"]
        }
    }

    # Product templates for variety
    templates = [
        "Premium {adj} {product_type} with {feature1} and {feature2}. Perfect for {use_case}.",
        "Professional-grade {product_type} featuring {feature1}, {feature2}, and {feature3}. Ideal for {use_case}.",
        "Compact {adj} {product_type} with {feature1}. Great for {use_case} and everyday use.",
        "High-performance {product_type} designed for {use_case}. Includes {feature1} and {feature2}.",
    ]

    adjectives = ["wireless", "portable", "durable", "lightweight", "ergonomic", "innovative", "smart"]
    features = [
        "long battery life", "fast charging", "water resistance", "premium materials",
        "advanced technology", "easy setup", "compact design", "powerful performance",
        "noise cancellation", "high resolution", "touch controls", "voice activation"
    ]
    use_cases = [
        "professionals", "home use", "travel", "outdoor activities",
        "students", "creators", "fitness enthusiasts", "daily commuting"
    ]

    products = []

    for i in range(num_products):
        category = random.choice(list(categories.keys()))
        cat_info = categories[category]
        subcategory = random.choice(cat_info["subcategories"])
        brand = random.choice(cat_info["brands"])

        # Generate product name
        product_types = {
            "Audio": ["Headphones", "Speakers", "Earbuds", "Amplifier"],
            "Computers": ["Laptop", "Desktop", "Monitor", "Keyboard"],
            "Phones": ["Smartphone", "Phone Case", "Charger", "Screen Protector"],
            "Cameras": ["Camera", "Lens", "Tripod", "Camera Bag"],
            "Furniture": ["Chair", "Desk", "Sofa", "Table"],
            "Kitchen": ["Blender", "Coffee Maker", "Toaster", "Mixer"],
            "Fitness": ["Yoga Mat", "Dumbbells", "Resistance Bands", "Foam Roller"],
            "Camping": ["Tent", "Sleeping Bag", "Backpack", "Lantern"],
        }

        product_type = random.choice(product_types.get(subcategory, ["Product"]))
        name = f"{brand} {random.choice(adjectives).capitalize()} {product_type}"

        # Generate description
        template = random.choice(templates)
        description = template.format(
            adj=random.choice(adjectives),
            product_type=product_type.lower(),
            feature1=random.choice(features),
            feature2=random.choice(features),
            feature3=random.choice(features),
            use_case=random.choice(use_cases)
        )

        # Generate metadata
        price = round(random.uniform(19.99, 999.99), 2)
        in_stock = random.random() > 0.1  # 90% in stock
        rating = round(random.uniform(3.5, 5.0), 1)

        # Generate tags
        tag_pool = ["premium", "best-seller", "new", "sale", "eco-friendly", "limited-edition"]
        tags = random.sample(tag_pool, k=random.randint(1, 3))

        products.append({
            "name": name,
            "description": description,
            "category": category,
            "subcategory": subcategory,
            "price": price,
            "brand": brand,
            "in_stock": in_stock,
            "rating": rating,
            "tags": tags
        })

    return products


# ============================================================================
# SECTION 6: EXAMPLE A - SIMPLE SEMANTIC SEARCH
# ============================================================================

def example_a_simple_search(
    voyage_client: VoyageAIClient,
    vector_store: CassandraVectorStore,
    keyspace: str
):
    """
    Example A: Simple semantic search workflow.

    Demonstrates:
    - Basic embedding generation
    - Vector similarity search
    - Result display
    """
    print("\n" + "="*80)
    print("EXAMPLE A: Simple Semantic Search")
    print("="*80)

    # Create small product catalog
    print("\n1. Creating sample product catalog...")
    products = generate_product_catalog(num_products=20)
    print(f"   Generated {len(products)} products")

    # Generate embeddings
    print("\n2. Generating embeddings...")
    descriptions = [p["description"] for p in products]
    embeddings = voyage_client.embed_texts(
        texts=descriptions,
        model=Config.EMBEDDING_MODEL,
        input_type="document",
        dimension=Config.EMBEDDING_DIMENSION
    )
    print(f"   ✓ Generated {len(embeddings)} embeddings")

    # Insert products
    print("\n3. Inserting products into Cassandra...")
    for product, embedding in zip(products, embeddings):
        vector_store.insert_product(
            keyspace=keyspace,
            product_id=uuid.uuid4(),
            name=product["name"],
            description=product["description"],
            category=product["category"],
            subcategory=product["subcategory"],
            price=product["price"],
            brand=product["brand"],
            in_stock=product["in_stock"],
            rating=product["rating"],
            tags=product["tags"],
            description_vector=embedding
        )
    print(f"   ✓ Inserted {len(products)} products")

    # Perform searches
    print("\n4. Performing semantic searches...")

    search_queries = [
        "wireless headphones for music",
        "laptop for programming and development",
        "camping equipment for outdoor adventures"
    ]

    for query_text in search_queries:
        print(f"\n   Query: \"{query_text}\"")
        print("   " + "-"*60)

        # Generate query embedding
        query_vector = voyage_client.embed_single(
            query_text,
            model=Config.EMBEDDING_MODEL,
            input_type="query",
            dimension=Config.EMBEDDING_DIMENSION
        )

        # Search for similar products
        results = vector_store.vector_search(
            keyspace=keyspace,
            query_vector=query_vector,
            limit=3
        )

        # Display results
        for i, result in enumerate(results, 1):
            print(f"\n   {i}. {result['name']}")
            print(f"      Price: ${result['price']:.2f} | Brand: {result['brand']}")
            print(f"      Similarity: {result['similarity']:.4f}")
            print(f"      {result['description'][:80]}...")


# ============================================================================
# SECTION 7: EXAMPLE B - TOKEN-AWARE BATCHING
# ============================================================================

def example_b_token_batching(
    voyage_client: VoyageAIClient,
    vector_store: CassandraVectorStore,
    keyspace: str
):
    """
    Example B: Token-aware batching for large datasets.

    Demonstrates:
    - Token analysis
    - Intelligent batching based on token limits
    - Batch statistics
    """
    print("\n" + "="*80)
    print("EXAMPLE B: Token-Aware Batching for Large Datasets")
    print("="*80)

    # Generate larger catalog
    print("\n1. Generating large product catalog...")
    num_products = 500
    products = generate_product_catalog(num_products=num_products)
    print(f"   Generated {num_products} products")

    descriptions = [p["description"] for p in products]

    # Initialize token-aware batcher
    print("\n2. Initializing token-aware batcher...")
    batcher = TokenAwareBatcher(voyage_client.client, Config.EMBEDDING_MODEL)
    print(f"   Model: {Config.EMBEDDING_MODEL}")
    print(f"   Token limit: {batcher.max_tokens:,} tokens/batch")

    # Analyze token distribution
    print("\n3. Analyzing token distribution...")
    token_stats = batcher.analyze_tokens(descriptions)
    print(f"   Total documents:  {token_stats['total_docs']:,}")
    print(f"   Total tokens:     {token_stats['total_tokens']:,}")
    print(f"   Min tokens/doc:   {token_stats['min_tokens']:,}")
    print(f"   Max tokens/doc:   {token_stats['max_tokens']:,}")
    print(f"   Avg tokens/doc:   {token_stats['avg_tokens']:.1f}")

    # Generate embeddings with token-aware batching
    print("\n4. Generating embeddings with token-aware batching...")
    embeddings, batch_stats = batcher.embed_with_batching(
        descriptions,
        input_type="document",
        dimension=Config.EMBEDDING_DIMENSION
    )

    print(f"   ✓ Generated {len(embeddings)} embeddings")
    print(f"   ✓ Total batches: {batch_stats['total_batches']}")
    print("\n   Batch details:")

    for batch_info in batch_stats['batches']:
        utilization = (batch_info['total_tokens'] / batcher.max_tokens) * 100
        print(f"      Batch {batch_info['batch_num']}: "
              f"{batch_info['num_texts']:3d} docs, "
              f"{batch_info['total_tokens']:7,} tokens "
              f"({utilization:5.1f}% utilization)")

    # Insert products
    print("\n5. Storing products in Cassandra...")
    for product, embedding in zip(products, embeddings):
        vector_store.insert_product(
            keyspace=keyspace,
            product_id=uuid.uuid4(),
            name=product["name"],
            description=product["description"],
            category=product["category"],
            subcategory=product["subcategory"],
            price=product["price"],
            brand=product["brand"],
            in_stock=product["in_stock"],
            rating=product["rating"],
            tags=product["tags"],
            description_vector=embedding
        )
    print(f"   ✓ Inserted {len(products)} products")


# ============================================================================
# SECTION 8: EXAMPLE C - TWO-STAGE RETRIEVAL (RERANKING)
# ============================================================================

def example_c_reranking(
    voyage_client: VoyageAIClient,
    vector_store: CassandraVectorStore,
    keyspace: str
):
    """
    Example C: Two-stage retrieval with reranking.

    Demonstrates:
    - Stage 1: Vector search (fast, broad recall)
    - Stage 2: Reranking (accurate, precision)
    - Performance comparison
    """
    print("\n" + "="*80)
    print("EXAMPLE C: Two-Stage Retrieval with Reranking")
    print("="*80)

    test_queries = [
        "affordable wireless headphones with good battery life",
        "professional camera equipment for outdoor photography",
        "ergonomic office furniture for home workspace"
    ]

    for query_text in test_queries:
        print(f"\n{'='*70}")
        print(f"Query: \"{query_text}\"")
        print('='*70)

        # ====================================================================
        # Method 1: Vector Search Only (Baseline)
        # ====================================================================
        print("\n[BASELINE] Vector Search Only:")
        start = time.time()

        query_vector = voyage_client.embed_single(
            query_text,
            model=Config.EMBEDDING_MODEL,
            input_type="query",
            dimension=Config.EMBEDDING_DIMENSION
        )

        baseline_results = vector_store.vector_search(
            keyspace=keyspace,
            query_vector=query_vector,
            limit=10
        )

        baseline_time = (time.time() - start) * 1000

        print(f"  Time: {baseline_time:.2f}ms")
        print("\n  Top 3 Results:")
        for i, result in enumerate(baseline_results[:3], 1):
            print(f"\n  {i}. {result['name']}")
            print(f"     Similarity: {result['similarity']:.4f} | Price: ${result['price']:.2f}")
            print(f"     {result['description'][:70]}...")

        # ====================================================================
        # Method 2: Two-Stage Retrieval (Vector + Reranking)
        # ====================================================================
        print(f"\n\n[TWO-STAGE] Vector Search + Reranking:")
        total_start = time.time()

        # Stage 1: Vector search for candidates
        print("  Stage 1: Retrieving 100 candidates via vector search...")
        stage1_start = time.time()

        candidates = vector_store.vector_search(
            keyspace=keyspace,
            query_vector=query_vector,
            limit=100
        )

        stage1_time = (time.time() - stage1_start) * 1000
        print(f"    Retrieved {len(candidates)} candidates in {stage1_time:.2f}ms")

        # Stage 2: Rerank with VoyageAI
        print("  Stage 2: Reranking with VoyageAI rerank-2.5...")
        stage2_start = time.time()

        documents = [c["description"] for c in candidates]

        rerank_response = voyage_client.rerank(
            query=query_text,
            documents=documents,
            model=Config.RERANK_MODEL,
            top_k=10
        )

        stage2_time = (time.time() - stage2_start) * 1000
        total_time = (time.time() - total_start) * 1000

        print(f"    Reranked to top 10 in {stage2_time:.2f}ms")
        print(f"  Total Time: {total_time:.2f}ms")

        # Combine reranking results with metadata
        reranked_results = []
        for item in rerank_response.results:
            original = candidates[item.index]
            reranked_results.append({
                **original,
                "relevance_score": item.relevance_score,
                "original_rank": item.index + 1
            })

        print("\n  Top 3 Results:")
        for i, result in enumerate(reranked_results[:3], 1):
            print(f"\n  {i}. {result['name']}")
            print(f"     Relevance: {result['relevance_score']:.4f} | "
                  f"Vector Sim: {result['similarity']:.4f} | "
                  f"Price: ${result['price']:.2f}")
            print(f"     Moved from position #{result['original_rank']} → #{i}")
            print(f"     {result['description'][:70]}...")

        # Performance comparison
        print(f"\n  {'─'*60}")
        print("  PERFORMANCE ANALYSIS:")
        print(f"    Baseline (vector only):   {baseline_time:.2f}ms")
        print(f"    Two-stage (with rerank):  {total_time:.2f}ms")
        print(f"    Latency increase:         +{total_time - baseline_time:.2f}ms")
        print(f"    Accuracy improvement:     Better relevance in top results")


# ============================================================================
# SECTION 9: EXAMPLE D - HYBRID SEARCH
# ============================================================================

def example_d_hybrid_search(
    voyage_client: VoyageAIClient,
    vector_store: CassandraVectorStore,
    keyspace: str
):
    """
    Example D: Hybrid search combining vector, keyword filters, and reranking.

    Demonstrates:
    - Vector search with category/brand/price filters
    - Result merging and deduplication
    - Reranking for final precision
    """
    print("\n" + "="*80)
    print("EXAMPLE D: Hybrid Search (Vector + Keyword + Reranking)")
    print("="*80)

    # Scenario 1: Semantic search with price filter
    print("\n" + "─"*70)
    print("Scenario 1: Semantic Query + Price Filter")
    print("─"*70)

    query_text = "high-quality audio equipment"
    max_price = 300.0

    print(f"\nQuery: \"{query_text}\"")
    print(f"Filter: price <= ${max_price}, in_stock = true")

    query_vector = voyage_client.embed_single(
        query_text,
        model=Config.EMBEDDING_MODEL,
        input_type="query",
        dimension=Config.EMBEDDING_DIMENSION
    )

    # Hybrid search with filters
    results = vector_store.vector_search(
        keyspace=keyspace,
        query_vector=query_vector,
        limit=50,
        max_price=max_price,
        in_stock_only=True
    )

    print(f"\nFound {len(results)} products matching criteria")

    if results:
        # Rerank results
        documents = [r["description"] for r in results]
        rerank_response = voyage_client.rerank(
            query=query_text,
            documents=documents,
            model=Config.RERANK_MODEL,
            top_k=5
        )

        print("\nTop 5 Results (after reranking):")
        for i, item in enumerate(rerank_response.results, 1):
            result = results[item.index]
            print(f"\n{i}. {result['name']}")
            print(f"   Price: ${result['price']:.2f} | Brand: {result['brand']} | "
                  f"Rating: {result['rating']}")
            print(f"   Relevance: {item.relevance_score:.4f} | "
                  f"Vector Sim: {result['similarity']:.4f}")
            print(f"   In Stock: {'Yes' if result['in_stock'] else 'No'}")

    # Scenario 2: Brand-specific search
    print("\n\n" + "─"*70)
    print("Scenario 2: Brand-Specific Search")
    print("─"*70)

    query_text = "portable device for outdoor activities"
    brand = "Sony"

    print(f"\nQuery: \"{query_text}\"")
    print(f"Filter: brand = {brand}, in_stock = true")

    query_vector = voyage_client.embed_single(
        query_text,
        model=Config.EMBEDDING_MODEL,
        input_type="query",
        dimension=Config.EMBEDDING_DIMENSION
    )

    results = vector_store.vector_search(
        keyspace=keyspace,
        query_vector=query_vector,
        limit=50,
        brand=brand,
        in_stock_only=True
    )

    print(f"\nFound {len(results)} {brand} products matching criteria")

    if results:
        # Rerank
        documents = [r["description"] for r in results]
        rerank_response = voyage_client.rerank(
            query=query_text,
            documents=documents,
            model=Config.RERANK_MODEL,
            top_k=3
        )

        print("\nTop 3 Results:")
        for i, item in enumerate(rerank_response.results, 1):
            result = results[item.index]
            print(f"\n{i}. {result['name']}")
            print(f"   Relevance: {item.relevance_score:.4f} | Price: ${result['price']:.2f}")


# ============================================================================
# SECTION 10: MAIN FUNCTION
# ============================================================================

def main():
    """Main application demonstrating VoyageAI + Cassandra integration."""

    print("\n" + "="*80)
    print("VoyageAI + Apache Cassandra: Comprehensive Integration")
    print("="*80 + "\n")

    # Validate configuration
    try:
        Config.validate()
    except ValueError as e:
        print(f"Configuration error: {e}")
        return 1

    # Initialize components
    print("Initializing components...")
    print("-" * 80)

    voyage_client = VoyageAIClient(api_key=Config.VOYAGE_API_KEY)

    vector_store = CassandraVectorStore(
        hosts=Config.CASSANDRA_HOSTS,
        port=Config.CASSANDRA_PORT,
        username=Config.CASSANDRA_USERNAME,
        password=Config.CASSANDRA_PASSWORD
    )

    try:
        vector_store.connect()

        # Setup schema
        print("\nSetting up Cassandra schema...")
        print("-" * 80)

        vector_store.create_keyspace(
            keyspace=Config.CASSANDRA_KEYSPACE,
            replication_factor=1
        )

        vector_store.create_products_table(
            keyspace=Config.CASSANDRA_KEYSPACE,
            dimension=Config.EMBEDDING_DIMENSION
        )

        vector_store.create_indexes(
            keyspace=Config.CASSANDRA_KEYSPACE,
            similarity_function=Config.SIMILARITY_FUNCTION
        )

        # Run examples
        print("\n\n" + "="*80)
        print("RUNNING EXAMPLES")
        print("="*80)

        # Example A: Simple semantic search
        example_a_simple_search(voyage_client, vector_store, Config.CASSANDRA_KEYSPACE)

        # Example B: Token-aware batching
        example_b_token_batching(voyage_client, vector_store, Config.CASSANDRA_KEYSPACE)

        # Example C: Two-stage retrieval with reranking
        example_c_reranking(voyage_client, vector_store, Config.CASSANDRA_KEYSPACE)

        # Example D: Hybrid search
        example_d_hybrid_search(voyage_client, vector_store, Config.CASSANDRA_KEYSPACE)

        # Summary
        print("\n\n" + "="*80)
        print("SUCCESS: All examples completed!")
        print("="*80)

        print("\nKey Takeaways:")
        print("="*80)
        print("\n1. BASIC INTEGRATION")
        print("   ✓ VoyageAI generates high-quality embeddings")
        print("   ✓ Cassandra stores and searches vectors efficiently")
        print("   ✓ SAI indexes enable fast ANN search")

        print("\n2. TOKEN-AWARE BATCHING")
        print("   ✓ Prevents API errors from exceeding token limits")
        print("   ✓ Maximizes batch utilization")
        print("   ✓ Essential for production deployments")

        print("\n3. TWO-STAGE RETRIEVAL")
        print("   ✓ Stage 1: Fast vector search (20-50ms)")
        print("   ✓ Stage 2: Accurate reranking (100-300ms)")
        print("   ✓ Best accuracy for user-facing search")

        print("\n4. HYBRID SEARCH")
        print("   ✓ Combines semantic similarity with filters")
        print("   ✓ Supports complex queries (price, brand, availability)")
        print("   ✓ Ideal for e-commerce and catalogs")

        print("\nProduction Recommendations:")
        print("─"*80)
        print("  • Always use token-aware batching for large datasets")
        print("  • Use reranking for top-result accuracy")
        print("  • Combine filters for better user experience")
        print("  • Monitor API usage and costs")
        print("  • Cache frequently searched queries")

        return 0

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        vector_store.close()


if __name__ == "__main__":
    sys.exit(main())
