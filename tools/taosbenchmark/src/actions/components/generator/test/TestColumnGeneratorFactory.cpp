#include <iostream>
#include <cassert>
#include "ColumnGeneratorFactory.h"


void test_factory_create_random_column_generator() {
    ColumnConfig config;
    config.gen_type = std::string("random");
    config.type = "int";
    config.min = 5;
    config.max = 15;

    auto generator = ColumnGeneratorFactory::create(config);
    assert(generator != nullptr); // Ensure the generator is created successfully

    for (int i = 0; i < 100; ++i) {
        ColumnType value = generator->generate();
        assert(std::holds_alternative<int>(value));
        int int_value = std::get<int>(value);
        assert(int_value >= 5 && int_value < 15);
    }

    std::cout << "test_factory_create_random_column_generator passed.\n";
}

int main() {
    test_factory_create_random_column_generator();

    std::cout << "All tests passed.\n";
    return 0;
}