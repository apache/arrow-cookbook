#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>

#include <filesystem>
#include <memory>

#include "common.h"

class DatasetReadingTest : public ::testing::Test {
 public:
  void SetUp() override {
    airquality_partitioned_dir_ =
        std::filesystem::temp_directory_path() / "cookbook_cpp_airquality";
    std::shared_ptr<arrow::fs::FileSystem> fs =
        std::make_shared<arrow::fs::LocalFileSystem>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Table> airquality,
                         ReadInAirQuality(fs.get()));
    WritePartitionedAirQuality(airquality, std::move(fs));
  }

  const std::string& airquality_basedir() { return airquality_partitioned_dir_; }

 private:
  void WritePartitionedAirQuality(const std::shared_ptr<arrow::Table>& airquality,
                                  std::shared_ptr<arrow::fs::FileSystem> fs) {
    std::shared_ptr<arrow::RecordBatchReader> table_reader =
        std::make_shared<arrow::TableBatchReader>(*airquality);

    std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder =
        arrow::dataset::ScannerBuilder::FromRecordBatchReader(std::move(table_reader));
    ASSERT_OK(scanner_builder->UseThreads(true));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::dataset::Scanner> scanner,
                         scanner_builder->Finish());

    std::shared_ptr<arrow::Schema> partitioning_schema = arrow::schema(
        {arrow::field("Month", arrow::int32()), arrow::field("Day", arrow::int32())});
    std::shared_ptr<arrow::dataset::PartitioningFactory> partitioning_factory =
        arrow::dataset::HivePartitioning::MakeFactory();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::dataset::Partitioning> partitioning,
                         partitioning_factory->Finish(partitioning_schema));

    std::shared_ptr<arrow::dataset::ParquetFileFormat> parquet_format =
        std::make_shared<arrow::dataset::ParquetFileFormat>();

    arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.filesystem = std::move(fs);
    write_options.partitioning = std::move(partitioning);
    write_options.base_dir = airquality_partitioned_dir_;
    write_options.basename_template = "chunk-{i}.parquet";
    write_options.file_write_options = parquet_format->DefaultWriteOptions();

    ASSERT_OK(
        arrow::dataset::FileSystemDataset::Write(write_options, std::move(scanner)));
  }

  static arrow::Result<std::shared_ptr<arrow::Table>> ReadInAirQuality(
      arrow::fs::FileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(std::string airquality_path,
                          FindTestDataFile("airquality.parquet"));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::RandomAccessFile> file,
                          fs->OpenInputFile(airquality_path));
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
        parquet::ParquetFileReader::Open(file);
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::FileReader::Make(
        arrow::default_memory_pool(), std::move(parquet_reader), &reader));
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(reader->ReadTable(&table));
    return table;
  }

  std::string airquality_partitioned_dir_;
};

TEST_F(DatasetReadingTest, DatasetRead) {
  StartRecipe("ListPartitionedDataset");
  const std::string& directory_base = airquality_basedir();

  // Create a filesystem
  std::shared_ptr<arrow::fs::LocalFileSystem> fs =
      std::make_shared<arrow::fs::LocalFileSystem>();

  // Create a file selector which describes which files are part of
  // the dataset.  This selector performs a recursive search of a base
  // directory which is typical with partitioned datasets.  You can also
  // create a dataset from a list of one or more paths.
  arrow::fs::FileSelector selector;
  selector.base_dir = directory_base;
  selector.recursive = true;

  // List out the files so we can see how our data is partitioned.
  // This step is not necessary for reading a dataset
  ASSERT_OK_AND_ASSIGN(std::vector<arrow::fs::FileInfo> file_infos,
                       fs->GetFileInfo(selector));
  int num_printed = 0;
  for (const auto& path : file_infos) {
    if (path.IsFile()) {
      rout << path.path().substr(directory_base.size()) << std::endl;
      if (++num_printed == 10) {
        rout << "..." << std::endl;
        break;
      }
    }
  }

  EndRecipe("ListPartitionedDataset");
  StartRecipe("CreatingADataset");
  // Create a file format which describes the format of the files.
  // Here we specify we are reading parquet files.  We could pick a different format
  // such as Arrow-IPC files or CSV files or we could customize the parquet format with
  // additional reading & parsing options.
  std::shared_ptr<arrow::dataset::ParquetFileFormat> format =
      std::make_shared<arrow::dataset::ParquetFileFormat>();

  // Create a partitioning factory.  A partitioning factory will be used by a dataset
  // factory to infer the partitioning schema from the filenames.  All we need to specify
  // is the flavor of partitioning which, in our case, is "hive".
  //
  // Alternatively, we could manually create a partitioning scheme from a schema.  This is
  // typically not necessary for hive partitioning as inference works well.
  std::shared_ptr<arrow::dataset::PartitioningFactory> partitioning_factory =
      arrow::dataset::HivePartitioning::MakeFactory();

  arrow::dataset::FileSystemFactoryOptions options;
  options.partitioning = partitioning_factory;

  // Create a dataset factory
  ASSERT_OK_AND_ASSIGN(
      std::shared_ptr<arrow::dataset::DatasetFactory> dataset_factory,
      arrow::dataset::FileSystemDatasetFactory::Make(fs, selector, format, options));

  // Create the dataset, this will scan the dataset directory to find all the files
  // and may scan some file metadata in order to determine the dataset schema.
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::dataset::Dataset> dataset,
                       dataset_factory->Finish());

  rout << "We discovered the following schema for the dataset:" << std::endl
       << std::endl
       << dataset->schema()->ToString() << std::endl;
  EndRecipe("CreatingADataset");
  StartRecipe("ScanningADataset");

  // Create a scanner
  arrow::dataset::ScannerBuilder scanner_builder(dataset);
  ASSERT_OK(scanner_builder.UseAsync(true));
  ASSERT_OK(scanner_builder.UseThreads(true));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::dataset::Scanner> scanner,
                       scanner_builder.Finish());

  // Scan the dataset.  There are a variety of other methods available on the scanner as
  // well
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<arrow::Table> table, scanner->ToTable());
  rout << "Read in a table with " << table->num_rows() << " rows and "
       << table->num_columns() << " columns";
  EndRecipe("ScanningADataset");
}
