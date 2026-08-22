# 作業計画: プロジェクト全体レビュー指摘の修正 (branch: 3.19.0)

- 作成日: 2026-08-22
- 対象ブランチ: `3.19.0`
- 前提: 2026-08-22 のプロジェクト全体レビュー（`./gradlew` 実行・`javap` による 3.19.0 API 確認込み）で挙がった指摘

## ユーザー確定事項（Q&A 結果）

| 論点 | 決定 |
|---|---|
| VCS 変更の範囲 | `gradlew` に実行権限を付与する / `scalardb.properties` を untrack する |
| `scalardb.properties` の扱い | **環境変数で上書き**する方式に変更（後述 2-1 を改訂）。デフォルトは `localhost` / `scalaradmin` / `scalaradmin` |
| `.idea/` の untrack | **実施しない**（現状のまま tracked で残す） |
| `application` プラグイン | **削除**する（6 つの `JavaExec` タスクだけで足りる） |
| テスト依存 (JUnit/Mockito) | **削除**する（`src/test/` は `.gitignore` 済みのため） |
| Java コード改修の深さ | **バグ修正 + クリーンアップのみ**（`args[]` 対応と `startReadOnly()` は行わない） |
| リモートリポジトリ | **削除して作り直す**（`gh repo delete` → 新規作成 → initial commit を push） |
| Java バージョン | toolchain を 17 に固定する（1-4 のとおり） |
| 環境変数名 | `SCALAR_DB_CONTACT_POINT` / `SCALAR_DB_USERNAME` / `SCALAR_DB_PASSWORD`（ホスト名のみ上書き） |

---

## 1. ビルド / 実行系

### 1-1. `gradlew` に実行権限を付与 *(指摘 1)*
- 現状: git 上のモードが `100644`。`./gradlew` が `Permission denied` で起動しない。
- 対応: `git update-index --chmod=+x gradlew`（+ ローカルも `chmod +x`）。

### 1-2. `application` プラグインを削除 *(指摘 2)*
- 現状: `mainClass` 未設定のため `./gradlew run` が失敗し、`distZip`/`distTar`/`startScripts` が壊れた成果物を生成する。
- 対応: `build.gradle` の `plugins` から `id 'application'` を削除。
- 副作用: `run` / `installDist` / `distZip` / `distTar` タスクが消える。本サンプルは main が 8 個あり、いずれも `run<X>Record` 系タスクで起動するため影響なし。

### 1-3. `runCreateTable` / `runDropTable` タスクを追加 *(指摘 3)*
- 現状: `CreateTable` / `DropTable` だけ Gradle タスクが無く、最初に必要なスキーマ操作が実行できない。
- 対応: 既存 6 タスクと同形式で 2 タスクを追加。`group` は `application` プラグイン削除後も文字列として有効なので、`'application'` のままとする。

### 1-4. Java toolchain 17 を明示 *(指摘 12)*
- 現状: `sourceCompatibility` も `java.toolchain` も無く、`PATH` 上の任意の JDK でビルドされる。README は JDK 17 を要求。
- 対応:
  ```gradle
  java {
      toolchain { languageVersion = JavaLanguageVersion.of(17) }
  }
  ```

### 1-5. テスト依存の削除 *(指摘 13)*
- 対応: `junit-bom` / `junit-jupiter` / `mockito-core` / `mockito-junit-jupiter` の 4 依存と、末尾の `test { useJUnitPlatform() }` ブロックを削除。
- `.gitignore` の `src/test/` はそのまま残す。

### 1-6. コメントアウトされた依存行の削除 *(指摘 11)*
- 対応: `//	implementation 'com.scalar-labs:scalardb:3.19.0'` を削除。
  `scalardb-cluster-java-client-sdk:3.19.0` が `scalardb-3.19.0` を推移的に取り込むことは dist の classpath で確認済み。

### 1-7. `build.gradle` 末尾に改行を追加 *(minor)*

---

## 2. 設定ファイル

### 2-1. `scalardb.properties` を環境変数方式にする *(指摘 14 / 方針変更)*

- **当初案（テンプレート化 + untrack）は破棄した。** ScalarDB 自身が環境変数展開に対応しているため、
  ファイルをそのまま tracked で残せる。
- 根拠（実機確認済み）: `com.scalar.db.config.ConfigUtils` は commons-text の `StringSubstitutor` を
  `env` / `sys` の lookup 付きで生成しており、**すべてのプロパティ値**が `${env:NAME:-default}` 構文で解決される。
- 対応: `scalardb.properties` を以下に書き換える。

  ```properties
  scalar.db.contact_points=indirect:${env:SCALAR_DB_CONTACT_POINT:-localhost}
  scalar.db.username=${env:SCALAR_DB_USERNAME:-scalaradmin}
  scalar.db.password=${env:SCALAR_DB_PASSWORD:-scalaradmin}
  ```

- `scalardb.properties.template` は作らない。`.gitignore` への追加も行わない（ファイルは tracked のまま）。
- **ユーザー影響**: これまでファイルに直書きしていた実接続先（ホスト / ユーザー / パスワード）は
  ファイルから消える。実行時は環境変数を export する必要がある。
- 検証結果:
  | 状態 | contact_points | username | password |
  |---|---|---|---|
  | 環境変数なし | `indirect:localhost` | `scalaradmin` | `scalaradmin` |
  | 環境変数あり | `indirect:10.0.0.5` | `alice` | `s3cret` |

### 2-2. `.idea/` — 対応しない *(指摘 15)*
- ユーザー判断により現状維持。

---

## 3. Java ソース（8 クラス）

### 3-1. リソースを必ず close する *(指摘 5)*
- 現状: `DistributedTransactionManager` / `DistributedTransactionAdmin` はいずれも 3.19.0 で `AutoCloseable`（`javap` で確認済み）だが、8 クラスすべて close していない。gRPC チャネルとスレッドプールが毎回リークする。
- 対応: try-with-resources 化。共通形は以下。

  CRUD 6 クラス:
  ```java
  TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
  try (DistributedTransactionManager manager = factory.getTransactionManager()) {
      DistributedTransaction transaction = manager.start();
      try {
          // ... CRUD ...
          transaction.commit();
          System.out.println("...");
      } catch (UnknownTransactionStatusException e) {
          throw e;
      } catch (Exception e) {
          try {
              transaction.rollback();
          } catch (Exception rollbackException) {
              e.addSuppressed(rollbackException);
          }
          throw e;
      }
  }
  ```

  `CreateTable` / `DropTable`:
  ```java
  TransactionFactory factory = TransactionFactory.create(SCALARDB_PROPERTIES);
  try (DistributedTransactionAdmin admin = factory.getTransactionAdmin()) {
      // ... admin 操作 ...
  }
  ```

### 3-2. catch 節で元例外を握り潰さない *(指摘 4)*
- 現状: `transaction.abort()` 自体が `AbortException` を投げると、元の例外 `e` が失われる。
- 対応: 上記 3-1 のとおり、rollback を内側の try/catch で囲み `addSuppressed` する。
- 併せて `abort()` → `rollback()` に統一する。
  （3.19.0 で `abort()` は deprecated ではなく `rollback()` の別名だが、正式名に寄せる）
- `commit()` 失敗のうち `UnknownTransactionStatusException` は rollback せずそのまま送出する。
  トランザクション状態は `DistributedTransactionManager#getState` で確認すべきものであり、rollback は誤り。理由をコメントで残す。

### 3-3. `CreateTable`: `createTable` に `ifNotExists` を渡す *(指摘 6)*
- 現状: 25 行目 `createCoordinatorTables(true)` / 28 行目 `createNamespace(NAME_SPACE_NAME, true)` は `true` を渡しているのに、40 行目の `createTable` だけ渡していない → 2 回目の実行がテーブル作成で失敗する。
- 対応: `admin.createTable(NAME_SPACE_NAME, TABLE_NAME, sample, true)`（オーバーロード存在確認済み）。

### 3-4. `DropTable`: coordinator テーブルも削除 *(指摘 7)*
- 現状: `CreateTable` が作る coordinator テーブルを `DropTable` が消さず、孤児として残る。
- 対応: `admin.dropCoordinatorTables(true)` を追加。実行順は namespace 削除の後とする。

### 3-5. デッドコードの削除 *(指摘 8)*
- `CreateTable` / `DropTable` の `DistributedTransactionManager manager;`（未代入・未使用）を削除。
- 同 2 クラスの `DistributedTransaction transaction = null;` と、到達不能な `if (transaction != null) transaction.abort();` を削除。
- CRUD 6 クラスの `DistributedTransactionManager manager;` 宣言は try-with-resources の変数に置き換える。

### 3-6. `UpsertRecord` の出力文言を修正 *(指摘 9)*
- `"Records were inserted"` → `"Record was upserted"`。
- 併せて 1 件しか操作しないクラスの複数形表記も単数に揃える
  （`InsertRecord`: `"Records were inserted"` → `"Record was inserted"`、
    `UpdateRecord`: `"Records were updated"` → `"Record was updated"`。
    `DeleteRecord` は既に `"Record was deleted"`）。

### 3-7. 未使用 import の削除 *(minor)*
| ファイル | 削除する import |
|---|---|
| `CreateTable.java` | `com.scalar.db.io.Key`, `java.util.Optional` |
| `DropTable.java` | `com.scalar.db.io.DataType` |
| `ScanRecord.java` | `java.util.Optional` |
| `UpdateRecord.java` | `java.util.Optional` |
| `UpsertRecord.java` | `com.scalar.db.api.Insert` |
| `InsertRecord.java` | `com.scalar.db.api.*` と重複する `DistributedTransaction` / `DistributedTransactionManager` の個別 import |

`UnknownTransactionStatusException` の import を 6 クラスに追加する。

### 3-8. `UpdateRecord` の `e.printStackTrace()` を削除 *(minor)*
- 8 クラス中このクラスだけの記述。例外はそのまま送出されるため出力が重複する。

### 3-9. インデントを 4 スペースに統一 *(minor)*
- 現状: `InsertRecord` / `UpdateRecord` / `UpsertRecord` / `DeleteRecord` がタブ、他 4 クラスが 4 スペース。
- `.idea/codeStyles/Project.xml` に Java 用の設定は無いため、多数派の 4 スペースに揃える。

---

## 4. README *(指摘 10)*

- Prerequisites のバージョンを `3.15.3 or later` → `3.19.0 or later` に更新。
- Directory Structure に `build.gradle` / `settings.gradle` / `scalardb.properties.template` を追記。
- 実行手順セクションを新設:
  1. `cp scalardb.properties.template scalardb.properties` して環境に合わせて編集
  2. `./gradlew runCreateTable`
  3. `./gradlew runInsertRecord` / `runGetRecord` / `runUpdateRecord` / `runUpsertRecord` / `runScanRecord` / `runDeleteRecord`
  4. `./gradlew runDropTable`
- `simple.sample.json` は ScalarDB Schema Loader 用の同等定義である旨を明記（`CreateTable.java` と内容一致を確認済み）。

---

## 5. 今回スコープ外（レビューで挙げたが対応しないもの）

| 項目 | 理由 |
|---|---|
| `args[]` によるパラメータ化（pk/ck/text/limit のハードコード） | ユーザー判断で見送り |
| `GetRecord` / `ScanRecord` の `startReadOnly()` 化（3.19 新機能） | ユーザー判断で見送り。最低要求クラスタバージョンが上がるため |
| `.idea/` の untrack | ユーザー判断で見送り |
| git 履歴からの認証情報除去 | 履歴書き換えを伴うため別途判断が必要 |
| `scalardb.properties` を classpath リソース化 | dist 生成をやめる（1-2）ため実害が消える |
| 8 クラスの共通定数・定型処理の基底クラス抽出 | サンプルとして 1 ファイル完結である方が読みやすいため |
| secondary index (`text_value`) を使う検索サンプルの追加 | 新機能追加であり修正の範囲外 |

---

## 6. 検証手順

1. `./gradlew clean build` が成功すること（`./gradlew` を直接叩けること = 指摘 1 の確認）
2. `./gradlew tasks --group application` に `runCreateTable` / `runDropTable` を含む 8 タスクが並ぶこと
3. `./gradlew compileJava` を `-Xlint:all` 付きで実行し、警告が出ないこと
4. `./gradlew run` タスクが存在しないこと（`application` プラグイン削除の確認）
5. `git status` / `git ls-files` で `scalardb.properties` が untracked、`scalardb.properties.template` が tracked、`gradlew` が `100755` になっていること
6. ワークツリーの `scalardb.properties`（実接続設定）が消えていないこと

**実クラスタへの接続が必要な動作確認（`runCreateTable` 〜 `runDropTable` の実行）は本計画では行わない。**
接続先の 3.19.0 クラスタが利用可能かどうかはユーザーに確認する。

---

## 7. コミット方針

- 作業はすべて現在のブランチ `3.19.0` 上で行う。
- コミット / push はユーザーの明示的な指示があるまで実施しない。
