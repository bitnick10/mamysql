#include <iostream>
#include <any>
#include <format>
#include <tuple>
#include <chrono>
#include <mysqlx/xdevapi.h>

export module mamysql;

export namespace mamysql {

std::string insert_value(int v) { return std::format("{}", v); }
std::string insert_value(float v) { return std::format("{}", v); }
std::string insert_value(double v) { return std::format("{}", v); }
std::string insert_value(const std::string& v) { return std::format("\"{}\"", v); }
std::string insert_value(const std::chrono::system_clock::time_point& v) { return std::format("FROM_UNIXTIME({})", std::chrono::duration_cast<std::chrono::seconds>(v.time_since_epoch()).count()); }

std::string insert_value(const std::optional<int>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }
std::string insert_value(const std::optional<float>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }
std::string insert_value(const std::optional<double>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }
std::string insert_value(const std::optional<std::string>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }
std::string insert_value(const std::optional<std::chrono::system_clock::time_point>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }

std::string insert_value(const std::any& any_value) {
	if (any_value.type() == typeid(int)) { return insert_value(std::any_cast<int>(any_value)); }
	if (any_value.type() == typeid(float)) { return insert_value(std::any_cast<float>(any_value)); }
	if (any_value.type() == typeid(double)) { return insert_value(std::any_cast<double>(any_value)); }
	if (any_value.type() == typeid(std::string)) { return insert_value(std::any_cast<std::string>(any_value)); }
	if (any_value.type() == typeid(std::chrono::system_clock::time_point)) { return insert_value(std::any_cast<std::chrono::system_clock::time_point>(any_value)); }

	if (any_value.type() == typeid(std::optional<int>)) { return insert_value(std::any_cast<std::optional<int>>(any_value)); }
	if (any_value.type() == typeid(std::optional<float>)) { return insert_value(std::any_cast<std::optional<float>>(any_value)); }
	if (any_value.type() == typeid(std::optional<double>)) { return insert_value(std::any_cast<std::optional<double>>(any_value)); }
	if (any_value.type() == typeid(std::optional<std::string>)) { return insert_value(std::any_cast<std::optional<std::string>>(any_value)); }
	if (any_value.type() == typeid(std::optional<std::chrono::system_clock::time_point>)) { return insert_value(std::any_cast<std::optional<std::chrono::system_clock::time_point>>(any_value)); }
}

std::vector<std::string> column_names(const std::map<std::string, std::any>& templateMA) {
	std::vector<std::string> ret;

	for (const auto& [key, value] : templateMA) {
		if (value.type() == typeid(std::chrono::system_clock::time_point) || value.type() == typeid(std::optional<std::chrono::system_clock::time_point>)) {
			ret.push_back(std::format("UNIX_TIMESTAMP({})", key));
		}
		else
			ret.push_back(key);
	}
	return ret;
}

std::string GetCreateTableQuery(const std::map<std::string, std::any>& obj, const std::string& tableName, const std::string& primaryKey, const std::string& engine) {
	std::string query = std::format("CREATE TABLE if not exists {} (", tableName);

	for (const auto& [key, value] : obj) {
		query += key;
		std::string coltype;
		if (value.type() == typeid(int)) { coltype = "INT"; }
		if (value.type() == typeid(float)) { coltype = "FLOAT"; }
		if (value.type() == typeid(double)) { coltype = "DOUBLE"; }
		if (value.type() == typeid(std::string)) { coltype = "VARCHAR(31)"; }
		if (value.type() == typeid(std::chrono::system_clock::time_point)) { coltype = "TIMESTAMP"; }

		if (value.type() == typeid(std::optional<int>)) { coltype = "INT"; }
		if (value.type() == typeid(std::optional<float>)) { coltype = "FLOAT"; }
		if (value.type() == typeid(std::optional<double>)) { coltype = "DOUBLE"; }
		if (value.type() == typeid(std::optional < std::string>)) { coltype = "VARCHAR(31)"; }
		if (value.type() == typeid(std::optional < std::chrono::system_clock::time_point>)) { coltype = "TIMESTAMP"; }
		query += " " + coltype + ",";
	}
	query += "PRIMARY KEY (" + primaryKey + "))";
	if (!engine.empty())
		query += " ENGINE=" + engine;
	query += ";";
	return query;
}

std::string ToInsertString(const std::map<std::string, std::any>& obj, const std::string& tableName, const std::string& insertType = "INSERT INTO") {
	std::string keys = "";
	std::string values = "";
	for (const auto& [key, any_value] : obj) {
		keys += key + ",";
		values += insert_value(any_value) + ",";
	}
	keys.pop_back();
	values.pop_back();
	std::string query = std::format("{} {}({}) VALUES({})", insertType, tableName, keys, values);
	return query;
}

void get_cell(std::any& any_value, const mysqlx::Value& v) {
	if (any_value.type() == typeid(int)) { if (!v.isNull()) any_value = v.get<int>(); }
	if (any_value.type() == typeid(float)) { if (!v.isNull()) any_value = v.get<float>(); }
	if (any_value.type() == typeid(double)) { if (!v.isNull()) any_value = v.get<double>(); }
	if (any_value.type() == typeid(std::string)) { if (!v.isNull()) any_value = v.get<std::string>(); }
	if (any_value.type() == typeid(std::chrono::system_clock::time_point)) { if (!v.isNull()) any_value = std::chrono::system_clock::from_time_t(time_t{ 0 }) + std::chrono::seconds(v.get<int>()); }

	if (any_value.type() == typeid(std::optional<int>)) { if (!v.isNull()) any_value = std::optional<int>(v.get<int>()); }
	if (any_value.type() == typeid(std::optional<float>)) { if (!v.isNull()) any_value = std::optional<float>(v.get<float>()); }
	if (any_value.type() == typeid(std::optional<double>)) { if (!v.isNull()) any_value = std::optional<double>(v.get<double>()); }
	if (any_value.type() == typeid(std::optional<std::string>)) { if (!v.isNull()) any_value = std::optional<std::string>(v.get<std::string>()); }
	if (any_value.type() == typeid(std::optional<std::chrono::system_clock::time_point>)) { if (!v.isNull()) any_value = std::optional<std::chrono::system_clock::time_point>(std::chrono::system_clock::from_time_t(time_t{ 0 }) + std::chrono::seconds(v.get<int>())); }
}

void read_row(std::map<std::string, std::any>& obj, mysqlx::Row& row) {
	int i = 0;
	for (auto& [key, any_value] : obj) {
		get_cell(any_value, row[i]);
		i++;
	}
}

inline std::string set_str(const std::string& name, int v) { return std::format("{}={}", name, v); }
inline std::string set_str(const std::string& name, float v) { return std::format("{}={}", name, v); }
inline std::string set_str(const std::string& name, double v) { return std::format("{}={}", name, v); }
inline std::string set_str(const std::string& name, const std::string& v) { return std::format("{}=\"{}\"", name, v); }
inline std::string set_str(const std::string& name, const std::chrono::system_clock::time_point& v) { return std::format("{}=FROM_UNIXTIME({})", name, std::chrono::duration_cast<std::chrono::seconds>(v.time_since_epoch()).count()); }

inline std::string set_str(const std::string& name, const std::optional<int>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }
inline std::string set_str(const std::string& name, const std::optional<float>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }
inline std::string set_str(const std::string& name, const std::optional<double>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }
inline std::string set_str(const std::string& name, const std::optional<std::string>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }
inline std::string set_str(const std::string& name, const std::optional<std::chrono::system_clock::time_point>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }

std::string set_str(const std::string& name, const std::any& v) {
	if (v.type() == typeid(int)) { return set_str(name, std::any_cast<int>(v)); }
	if (v.type() == typeid(float)) { return set_str(name, std::any_cast<float>(v)); }
	if (v.type() == typeid(double)) { return set_str(name, std::any_cast<double>(v)); }
	if (v.type() == typeid(std::string)) { return set_str(name, std::any_cast<std::string>(v)); }
	if (v.type() == typeid(std::chrono::system_clock::time_point)) { return set_str(name, std::any_cast<std::chrono::system_clock::time_point>(v)); }

	if (v.type() == typeid(std::optional<int>)) { return set_str(name, std::any_cast<std::optional<int>>(v)); }
	if (v.type() == typeid(std::optional<float>)) { return set_str(name, std::any_cast<std::optional<float>>(v)); }
	if (v.type() == typeid(std::optional<double>)) { return set_str(name, std::any_cast<std::optional<double>>(v)); }
	if (v.type() == typeid(std::optional<std::string>)) { return set_str(name, std::any_cast<std::optional<std::string>>(v)); }
	if (v.type() == typeid(std::optional<std::chrono::system_clock::time_point>)) { return set_str(name, std::any_cast<std::optional<std::chrono::system_clock::time_point>>(v)); }
}

void update_sets(const std::map<std::string, std::any>& obj, std::string& sets) {
	for (auto& [key, any_value] : obj) {
		std::string set = set_str(key, any_value);
		if (!set.empty()) {
			sets += set + ",";
		}
	}
	if (sets.back() == ',') sets.pop_back();
}

void primary_column_assignments(const std::map<std::string, std::any>& obj, const std::vector<std::string>& primary_keys, std::string& condition) {
	int i = 0;
	for (auto& [key, any_value] : obj) {
		if (std::find(primary_keys.begin(), primary_keys.end(), key) != primary_keys.end())
		{
			i++;
			condition += key;
			condition += "=" + insert_value(any_value);
			if (condition.back() == ',') condition.pop_back();
			if (i < primary_keys.size()) condition += " AND ";
		}
	}
}

class Session {
public:
	std::unique_ptr<mysqlx::Session> sess;
	Session(const std::tuple<std::string, std::string>& addr_pwd) {
		sess = std::unique_ptr<mysqlx::Session>(new mysqlx::Session(std::get<0>(addr_pwd), 33060, "root", std::get<1>(addr_pwd)));
	}
	Session(const std::tuple<std::string, std::string>& addr_pwd, const std::string schemaName) {
		sess = std::unique_ptr<mysqlx::Session>(new mysqlx::Session(std::get<0>(addr_pwd), 33060, "root", std::get<1>(addr_pwd)));
		if (!schemaName.empty())
			sess->sql("USE " + schemaName).execute();
	}
	Session(const std::string& addr, const std::string& pwd) {
		sess = std::unique_ptr<mysqlx::Session>(new mysqlx::Session(addr, 33060, "root", pwd));
	}
	bool table_exists(const std::string schema, const std::string& table) {
		if (!schema.empty())
			sess->sql("USE " + schema).execute();
		auto query = std::format("SELECT* FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}' LIMIT 1;", schema, table);
		auto res = sess->sql(query).execute();
		return res.count() != 0;
	}
	void CreateTableIfNotExists(const std::map<std::string, std::any>& obj, const std::string& tableName, const std::string& primaryKey, const std::string& engine) {
		std::string query;
		try {
			query = GetCreateTableQuery(obj, tableName, primaryKey, engine);
			sess->sql(query).execute();
		}
		catch (std::exception e) {
			std::cout << e.what() << "\n" << "error from CreateTableI:\n";
			std::cout << query << "\n";
		}
	}
	void ReplaceInto(const std::string& tableName, const std::map<std::string, std::any>& value) {
		std::string query;
		try {
			query = ToInsertString(value, tableName, "REPLACE INTO");
			sess->sql(query).execute();
		}
		catch (std::exception e) {
			std::cout << e.what() << "\n" << "error from ReplaceInto:\n";
			std::cout << query << "\n";
		}
	}
	std::vector<std::map<std::string, std::any>> Select(const std::string& tableName, const std::string& condition, const std::map<std::string, std::any>& templateOM) {
		std::vector<std::string> names = column_names(templateOM);
		std::string selects = "SELECT ";
		for (auto& name : names) {
			selects += name + ",";
		}
		selects.pop_back();
		auto query = std::format("{} FROM {}", selects, tableName);
		if (!condition.empty()) { query += " WHERE " + condition; }

		std::vector<std::map<std::string, std::any>> ret;

		try {
			auto res = sess->sql(query).execute();
			std::list<mysqlx::Row> rows = res.fetchAll();
			for (auto& row : rows) {
				std::map<std::string, std::any> object = templateOM;
				read_row(object, row);
				ret.push_back(object);
			}
		}
		catch (std::exception e) {
			std::cout << e.what() << "\n" << "error from Select:\n";
			std::cout << query << "\n";
			throw e;
		}
		return ret;
	}
	void Update(const std::map<std::string, std::any>& object, const std::string& tableName, const std::vector<std::string>& primary_keys, const std::string& condition = "") {
		std::string query;
		try {
			std::string sets;
			update_sets(object, sets);
			std::string condition;
			primary_column_assignments(object, primary_keys, condition);
			query = std::format("UPDATE {} SET {} WHERE {}", tableName, sets, condition);
			sess->sql(query).execute();
		}
		catch (std::exception e) {
			std::cout << e.what() << "\n" << "error from Update:\n";
			std::cout << query << "\n";
		}
	}
};

}
