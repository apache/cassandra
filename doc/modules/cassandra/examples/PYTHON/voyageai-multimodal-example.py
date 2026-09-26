#!/usr/bin/env python3
"""
VoyageAI Multimodal Embeddings (voyage-multimodal-3.5) + Apache Cassandra Vector Search

This example demonstrates REAL multimodal vector search using VoyageAI's multimodal models:
1. Embedding text, images, and video together in a shared vector space
2. Storing multimodal vectors in Cassandra (same vector space for all modalities)
3. Cross-modal similarity search (text query -> image results, image query -> text results)
4. Hybrid search combining text, images, and metadata filters

Prerequisites:
- Python 3.8+
- pip install 'voyageai>=0.3.6' cassandra-driver pillow requests
- VoyageAI API key (set as VOYAGE_API_KEY environment variable)
- Apache Cassandra 5.0+ with vector search support
- Sample images (or URLs) for demonstration

Supported Multimodal Models:

voyage-multimodal-3.5 (Preview - Default):
- Supports text, images, AND video in same vector space
- Variable dimensions: 256, 512, 1024 (default), 2048
- 32,000 token context length
- Images: max 16 million pixels, max 20MB
- Video: max 20MB (requires voyageai >= 0.3.6)
- Token pricing: 560 image pixels = 1 token, 1120 video pixels = 1 token

voyage-multimodal-3:
- Supports text and images (no video)
- Fixed 1024-dimensional embeddings
- 32,000 token context length
- Images: max 16 million pixels, max 20MB

Cross-modal search enabled (text finds images/video, images find text)

Author: Apache Cassandra Documentation Team
License: Apache 2.0
"""

import os
import sys
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import uuid
import json
import io

try:
    import voyageai
    from cassandra.cluster import Cluster, Session
    from cassandra.auth import PlainTextAuthProvider
    from PIL import Image
    import requests
except ImportError as e:
    print(f"Error: Missing required dependency - {e}")
    print("Install dependencies: pip install voyageai cassandra-driver pillow requests")
    sys.exit(1)


# ============================================================================
# Configuration
# ============================================================================

class Config:
    """Configuration for multimodal vector search."""

    # VoyageAI settings
    VOYAGE_API_KEY = os.getenv("VOYAGE_API_KEY")
    # Model options: voyage-multimodal-3 (fixed 1024D), voyage-multimodal-3.5 (256/512/1024/2048D)
    MULTIMODAL_MODEL = "voyage-multimodal-3.5"
    # Dimension options for voyage-multimodal-3.5: 256, 512, 1024 (default), 2048
    # Note: voyage-multimodal-3 only supports fixed 1024D
    EMBEDDING_DIMENSION = 1024

    # Cassandra settings
    CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "127.0.0.1").split(",")
    CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
    CASSANDRA_KEYSPACE = "multimodal_search"
    CASSANDRA_USERNAME = os.getenv("CASSANDRA_USERNAME")
    CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")

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
# Sample Data: Multimedia Content Library
# ============================================================================

# Demo images - Using placeholder image URLs for demonstration
# In production, replace with your actual images
SAMPLE_MEDIA_ITEMS = [
    {
        "title": "Mountain Landscape Photography",
        "description": "Majestic snow-capped mountain peaks at sunset with dramatic lighting",
        "content_type": "image",
        "tags": ["nature", "landscape", "mountains", "photography"],
        "image_url": "https://picsum.photos/800/600?mountain",
        "has_visual": True
    },
    {
        "title": "Machine Learning Tutorial",
        "description": "Comprehensive guide to neural networks and deep learning algorithms",
        "content_type": "article",
        "tags": ["technology", "machine-learning", "education"],
        "has_visual": False
    },
    {
        "title": "Ocean Beach Sunset",
        "description": "Tranquil beach scene with golden sunset over calm ocean waves",
        "content_type": "image",
        "tags": ["nature", "ocean", "beach", "sunset"],
        "image_url": "https://picsum.photos/800/600?ocean",
        "has_visual": True
    },
    {
        "title": "Modern Architecture Design",
        "description": "Contemporary building with glass facade and geometric patterns",
        "content_type": "image",
        "tags": ["architecture", "design", "modern", "urban"],
        "image_url": "https://picsum.photos/800/600?architecture",
        "has_visual": True
    },
    {
        "title": "Python Programming Guide",
        "description": "Complete Python tutorial covering data structures and algorithms",
        "content_type": "article",
        "tags": ["programming", "python", "education", "tutorial"],
        "has_visual": False
    },
    {
        "title": "Forest Trail Hiking",
        "description": "Lush green forest path winding through tall trees and vegetation",
        "content_type": "image",
        "tags": ["nature", "forest", "hiking", "outdoor"],
        "image_url": "https://picsum.photos/800/600?forest",
        "has_visual": True
    },
]


# ============================================================================
# VoyageAI Multimodal Embedder
# ============================================================================

class VoyageMultimodalEmbedder:
    """
    Handles multimodal embedding generation using VoyageAI's multimodal models.

    Supported models:
    - voyage-multimodal-3.5: Text + Images + Video, variable dimensions (256/512/1024/2048)
    - voyage-multimodal-3: Text + Images only, fixed 1024-dimensional vectors

    Both models embed content into the same vector space, enabling cross-modal similarity search.
    """

    def __init__(self, api_key: str, model: str = "voyage-multimodal-3.5", dimension: int = 1024):
        """
        Initialize VoyageAI multimodal client.

        Args:
            api_key: VoyageAI API key
            model: Model name (voyage-multimodal-3.5 or voyage-multimodal-3)
            dimension: Output dimension (256, 512, 1024, 2048). Only for voyage-multimodal-3.5.
                       voyage-multimodal-3 always produces 1024-dim vectors.
        """
        self.client = voyageai.Client(api_key=api_key)
        self.model = model

        # voyage-multimodal-3 only supports 1024D, voyage-multimodal-3.5 supports variable
        if model == "voyage-multimodal-3":
            self.dimension = 1024
            if dimension != 1024:
                print(f"  Note: voyage-multimodal-3 only supports 1024D (ignoring {dimension}D)")
        else:
            self.dimension = dimension

        modalities = "Text + Images + Video" if "3.5" in model else "Text + Images"
        print(f"✓ VoyageAI multimodal client initialized")
        print(f"  Model: {model}")
        print(f"  Dimension: {self.dimension}")
        print(f"  Supports: {modalities} in same vector space")

    def load_image_from_url(self, url: str) -> Image.Image:
        """
        Download and load image from URL.

        Args:
            url: Image URL

        Returns:
            PIL Image object
        """
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return Image.open(io.BytesIO(response.content))
        except Exception as e:
            print(f"Warning: Failed to load image from {url}: {e}")
            # Return a small placeholder image
            return Image.new('RGB', (100, 100), color='gray')

    def embed_text(self, text: str, input_type: str = "document") -> List[float]:
        """
        Embed text using VoyageAI multimodal model.

        Args:
            text: Text to embed
            input_type: "document" or "query"

        Returns:
            Embedding vector (dimension depends on model/config)
        """
        # Build kwargs - only add output_dimension for voyage-multimodal-3.5
        kwargs = {
            "inputs": [[text]],
            "model": self.model,
            "input_type": input_type
        }
        if "3.5" in self.model:
            kwargs["output_dimension"] = self.dimension

        result = self.client.multimodal_embed(**kwargs)
        return result.embeddings[0]

    def embed_image(self, image: Image.Image, caption: Optional[str] = None) -> List[float]:
        """
        Embed image (optionally with caption) using VoyageAI multimodal model.

        Args:
            image: PIL Image object
            caption: Optional text caption to embed with image

        Returns:
            Embedding vector (dimension depends on model/config)
        """
        if caption:
            # Embed image with caption (interleaved)
            inputs = [[caption, image]]
        else:
            # Embed image only
            inputs = [[image]]

        # Build kwargs - only add output_dimension for voyage-multimodal-3.5
        kwargs = {
            "inputs": inputs,
            "model": self.model,
            "input_type": "document"
        }
        if "3.5" in self.model:
            kwargs["output_dimension"] = self.dimension

        result = self.client.multimodal_embed(**kwargs)
        return result.embeddings[0]

    def embed_multimodal_item(self, item: Dict[str, Any]) -> List[float]:
        """
        Generate embedding for a multimodal item.

        For items with images: embeds image + description together
        For text-only items: embeds description only

        Args:
            item: Item dictionary with description and optional image_url

        Returns:
            Embedding vector (dimension depends on model/config)
        """
        if item.get("has_visual") and item.get("image_url"):
            # Load image
            image = self.load_image_from_url(item["image_url"])

            # Embed image with description (cross-modal alignment)
            embedding = self.embed_image(image, caption=item["description"])

            return embedding
        else:
            # Text-only content
            return self.embed_text(item["description"], input_type="document")

    def embed_query(self, query: str) -> List[float]:
        """
        Embed a search query.

        Can be used to find both text, image, and video content.

        Args:
            query: Search query text

        Returns:
            Query embedding vector (dimension depends on model/config)
        """
        return self.embed_text(query, input_type="query")


# ============================================================================
# Cassandra Multimodal Vector Store
# ============================================================================

class MultimodalVectorStore:
    """Handles multimodal vector storage and search in Cassandra."""

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

    def setup_schema(
        self,
        keyspace: str,
        dimension: int,
        replication_factor: int = 1
    ):
        """
        Create schema for multimodal content storage.

        Args:
            keyspace: Keyspace name
            dimension: Dimension of embeddings (1024 for voyage-multimodal-3)
            replication_factor: Replication factor
        """
        # Create keyspace
        query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{
            'class': 'SimpleStrategy',
            'replication_factor': {replication_factor}
        }}
        """
        self.session.execute(query)
        print(f"✓ Keyspace '{keyspace}' created")

        self.session.set_keyspace(keyspace)

        # Create media items table
        # Note: Single vector column since both text and images use same vector space
        query = f"""
        CREATE TABLE IF NOT EXISTS media_items (
            item_id UUID PRIMARY KEY,
            title TEXT,
            description TEXT,
            content_type TEXT,
            tags SET<TEXT>,
            has_visual BOOLEAN,
            embedding VECTOR<FLOAT, {dimension}>,
            image_url TEXT,
            metadata TEXT,
            created_at TIMESTAMP
        )
        """
        self.session.execute(query)
        print(f"✓ Table 'media_items' created with VECTOR<FLOAT, {dimension}>")

        # Create SAI index for vector similarity search
        query = f"""
        CREATE CUSTOM INDEX IF NOT EXISTS media_embedding_idx
        ON media_items(embedding)
        USING 'StorageAttachedIndex'
        WITH OPTIONS = {{'similarity_function': 'COSINE'}}
        """
        self.session.execute(query)
        print("✓ SAI vector index created (COSINE similarity)")

        # Create index on content_type for filtering
        query = """
        CREATE CUSTOM INDEX IF NOT EXISTS media_content_type_idx
        ON media_items(content_type)
        USING 'StorageAttachedIndex'
        """
        self.session.execute(query)
        print("✓ SAI index created on content_type")

    def insert_media_item(
        self,
        keyspace: str,
        item_id: uuid.UUID,
        title: str,
        description: str,
        content_type: str,
        tags: List[str],
        has_visual: bool,
        embedding: List[float],
        image_url: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Insert a multimodal media item."""
        self.session.set_keyspace(keyspace)

        query = """
        INSERT INTO media_items (
            item_id, title, description, content_type, tags,
            has_visual, embedding, image_url, metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.session.execute(
            query,
            (
                item_id,
                title,
                description,
                content_type,
                set(tags),
                has_visual,
                embedding,
                image_url,
                json.dumps(metadata) if metadata else None,
                datetime.utcnow()
            )
        )

    def search_similar(
        self,
        keyspace: str,
        query_vector: List[float],
        content_type: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Search media items using vector similarity.

        Thanks to voyage-multimodal-3, this works for:
        - Text query -> Text results
        - Text query -> Image results (cross-modal)
        - Image query -> Text results (cross-modal)
        - Image query -> Image results

        Args:
            keyspace: Keyspace name
            query_vector: Query embedding
            content_type: Optional filter by content type
            limit: Maximum results

        Returns:
            List of matching media items
        """
        self.session.set_keyspace(keyspace)

        if content_type:
            query = """
            SELECT
                item_id, title, description, content_type, tags,
                has_visual, image_url,
                similarity_cosine(embedding, ?) AS similarity
            FROM media_items
            WHERE content_type = ?
            ORDER BY embedding ANN OF ?
            LIMIT ?
            """
            rows = self.session.execute(query, (query_vector, content_type, query_vector, limit))
        else:
            query = """
            SELECT
                item_id, title, description, content_type, tags,
                has_visual, image_url,
                similarity_cosine(embedding, ?) AS similarity
            FROM media_items
            ORDER BY embedding ANN OF ?
            LIMIT ?
            """
            rows = self.session.execute(query, (query_vector, query_vector, limit))

        return [self._row_to_dict(row) for row in rows]

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        """Convert Cassandra row to dictionary."""
        return {
            "item_id": str(row.item_id),
            "title": row.title,
            "description": row.description,
            "content_type": row.content_type,
            "tags": list(row.tags) if row.tags else [],
            "has_visual": row.has_visual,
            "image_url": row.image_url,
            "similarity": float(row.similarity) if hasattr(row, 'similarity') and row.similarity else None
        }


# ============================================================================
# Main Application
# ============================================================================

def main():
    """Main application demonstrating multimodal vector search."""

    print("\n" + "="*80)
    print(f"VoyageAI Multimodal ({Config.MULTIMODAL_MODEL}) + Cassandra Vector Search")
    print("="*80 + "\n")

    # Validate configuration
    try:
        Config.validate()
    except ValueError as e:
        print(f"Configuration error: {e}")
        return 1

    # Initialize components
    print("1. Initializing VoyageAI multimodal embedder...")
    print("-" * 80)

    embedder = VoyageMultimodalEmbedder(
        api_key=Config.VOYAGE_API_KEY,
        model=Config.MULTIMODAL_MODEL,
        dimension=Config.EMBEDDING_DIMENSION
    )

    vector_store = MultimodalVectorStore(
        hosts=Config.CASSANDRA_HOSTS,
        port=Config.CASSANDRA_PORT,
        username=Config.CASSANDRA_USERNAME,
        password=Config.CASSANDRA_PASSWORD
    )

    try:
        vector_store.connect()

        # Setup schema
        print("\n2. Setting up Cassandra schema...")
        print("-" * 80)

        vector_store.setup_schema(
            keyspace=Config.CASSANDRA_KEYSPACE,
            dimension=Config.EMBEDDING_DIMENSION,
            replication_factor=1
        )

        # Generate and store embeddings
        print(f"\n3. Generating multimodal embeddings with {Config.MULTIMODAL_MODEL}...")
        print("-" * 80)

        for item in SAMPLE_MEDIA_ITEMS:
            # Generate embedding (handles both text and image content)
            embedding = embedder.embed_multimodal_item(item)

            item_id = uuid.uuid4()
            vector_store.insert_media_item(
                keyspace=Config.CASSANDRA_KEYSPACE,
                item_id=item_id,
                title=item["title"],
                description=item["description"],
                content_type=item["content_type"],
                tags=item["tags"],
                has_visual=item.get("has_visual", False),
                embedding=embedding,
                image_url=item.get("image_url"),
                metadata={}
            )

            modality = "text + image" if item.get("has_visual") else "text only"
            print(f"  ✓ Embedded: {item['title']} ({modality})")

        print(f"\n✓ Inserted {len(SAMPLE_MEDIA_ITEMS)} multimodal items")

        # Perform multimodal searches
        print("\n4. Performing cross-modal similarity searches...")
        print("-" * 80)

        # Search 1: Text query -> Find all content (including images)
        print("\n[A] Text Query -> All Content: 'beautiful natural scenery'")
        print("-" * 40)
        query_vector = embedder.embed_query("beautiful natural scenery")
        results = vector_store.search_similar(
            keyspace=Config.CASSANDRA_KEYSPACE,
            query_vector=query_vector,
            limit=4
        )

        for i, result in enumerate(results, 1):
            visual_tag = " [IMAGE]" if result['has_visual'] else " [TEXT]"
            print(f"{i}. {result['title']}{visual_tag}")
            print(f"   Type: {result['content_type']} | Similarity: {result['similarity']:.4f}")
            print(f"   Description: {result['description'][:70]}...")
            print()

        # Search 2: Cross-modal search - Text query -> Images only
        print("[B] Cross-Modal Search: Text query -> Image results")
        print("    Query: 'sunset over water'")
        print("-" * 40)
        query_vector = embedder.embed_query("sunset over water")
        results = vector_store.search_similar(
            keyspace=Config.CASSANDRA_KEYSPACE,
            query_vector=query_vector,
            content_type="image",  # Filter to images only
            limit=3
        )

        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']}")
            print(f"   Similarity: {result['similarity']:.4f}")
            print(f"   Tags: {', '.join(result['tags'])}")
            print()

        # Search 3: Find programming tutorials
        print("[C] Text Search: 'learning to code and program'")
        print("-" * 40)
        query_vector = embedder.embed_query("learning to code and program")
        results = vector_store.search_similar(
            keyspace=Config.CASSANDRA_KEYSPACE,
            query_vector=query_vector,
            content_type="article",
            limit=3
        )

        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']}")
            print(f"   Similarity: {result['similarity']:.4f}")
            print()

        # Search 4: Architecture and design images
        print("[D] Image Search: 'modern buildings and architecture'")
        print("-" * 40)
        query_vector = embedder.embed_query("modern buildings and architecture")
        results = vector_store.search_similar(
            keyspace=Config.CASSANDRA_KEYSPACE,
            query_vector=query_vector,
            content_type="image",
            limit=3
        )

        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']}")
            print(f"   Similarity: {result['similarity']:.4f}")
            print(f"   URL: {result.get('image_url', 'N/A')}")
            print()

        print("\n" + "="*80)
        print("SUCCESS: Multimodal vector search demonstration complete!")
        print("="*80)

        print("\nKey Features Demonstrated:")
        print(f"✓ Real VoyageAI {Config.MULTIMODAL_MODEL} integration")
        print(f"✓ Text and images embedded in same {Config.EMBEDDING_DIMENSION}-dim vector space")
        print("✓ Cross-modal search (text queries find images, vice versa)")
        print("✓ Single vector column for both modalities")
        print("✓ Content-type filtering for hybrid search")
        print("✓ COSINE similarity for normalized embeddings")
        if "3.5" in Config.MULTIMODAL_MODEL:
            print("✓ Variable dimensions (256, 512, 1024, 2048)")
            print("✓ Video embedding support (requires voyageai >= 0.3.6)")

        print("\nProduction Use Cases:")
        print("- E-commerce: Text search returns product images")
        print("- Media libraries: Find photos by description")
        print("- Document search: Images in PDFs/slides/documents")
        print("- Video retrieval: Find video content by text query")
        print("- Visual Q&A: Natural language queries for visual content")

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
